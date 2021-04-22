/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.sql.NodeUtils;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analysis.GroupingSetAnalysis;
import io.trino.sql.analyzer.Analysis.ResolvedWindow;
import io.trino.sql.analyzer.Analysis.SelectExpression;
import io.trino.sql.analyzer.FieldId;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DeleteNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableWriterNode.DeleteTarget;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionCall.NullTreatment;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.WindowFrame;
import io.trino.type.TypeCoercion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getMaxRecursionDepth;
import static io.trino.SystemSessionProperties.isSkipRedundantSort;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isNumericType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.GroupingOperationRewriter.rewriteGroupingOperation;
import static io.trino.sql.planner.LogicalPlanner.id;
import static io.trino.sql.planner.OrderingScheme.sortItemToSortOrder;
import static io.trino.sql.planner.PlanBuilder.newPlanBuilder;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static io.trino.sql.planner.plan.AggregationNode.groupingSets;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.DAY;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.YEAR;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static io.trino.sql.tree.WindowFrame.Type.GROUPS;
import static io.trino.sql.tree.WindowFrame.Type.RANGE;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class QueryPlanner
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final Metadata metadata;
    private final TypeCoercion typeCoercion;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;
    private final Optional<TranslationMap> outerContext;
    private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

    QueryPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            Metadata metadata,
            Optional<TranslationMap> outerContext,
            Session session,
            Map<NodeRef<Node>, RelationPlan> recursiveSubqueries)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");
        requireNonNull(outerContext, "outerContext is null");
        requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.metadata = metadata;
        this.typeCoercion = new TypeCoercion(metadata::getType);
        this.session = session;
        this.outerContext = outerContext;
        this.subqueryPlanner = new SubqueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, typeCoercion, outerContext, session, recursiveSubqueries);
        this.recursiveSubqueries = recursiveSubqueries;
    }

    public RelationPlan plan(Query query)
    {
        PlanBuilder builder = planQueryBody(query);

        List<Expression> orderBy = analysis.getOrderByExpressions(query);
        builder = subqueryPlanner.handleSubqueries(builder, orderBy, query);

        List<SelectExpression> selectExpressions = analysis.getSelectExpressions(query);
        List<Expression> outputs = selectExpressions.stream()
                .map(SelectExpression::getExpression)
                .collect(toImmutableList());
        builder = builder.appendProjections(Iterables.concat(orderBy, outputs), symbolAllocator, idAllocator);

        Optional<OrderingScheme> orderingScheme = orderingScheme(builder, query.getOrderBy(), analysis.getOrderByExpressions(query));
        builder = sort(builder, orderingScheme);
        builder = offset(builder, query.getOffset());
        builder = limit(builder, query.getLimit(), orderingScheme);
        builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

        return new RelationPlan(
                builder.getRoot(),
                analysis.getScope(query),
                computeOutputs(builder, outputs),
                outerContext);
    }

    public RelationPlan planExpand(Query query)
    {
        checkArgument(analysis.isExpandableQuery(query), "query is not registered as expandable");

        Union union = (Union) query.getQueryBody();
        ImmutableList.Builder<NodeAndMappings> recursionSteps = ImmutableList.builder();

        // plan anchor relation
        Relation anchorNode = union.getRelations().get(0);
        RelationPlan anchorPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, outerContext, session, recursiveSubqueries)
                .process(anchorNode, null);

        // prune anchor plan outputs to contain only the symbols exposed in the scope
        NodeAndMappings prunedAnchorPlan = pruneInvisibleFields(anchorPlan, idAllocator);
        anchorPlan = new RelationPlan(prunedAnchorPlan.getNode(), analysis.getScope(query), prunedAnchorPlan.getFields(), outerContext);

        recursionSteps.add(copy(anchorPlan.getRoot(), anchorPlan.getFieldMappings()));

        // plan recursion step
        Relation recursionStepRelation = union.getRelations().get(1);
        RelationPlan recursionStepPlan = new RelationPlanner(
                analysis,
                symbolAllocator,
                idAllocator,
                lambdaDeclarationToSymbolMap,
                metadata,
                outerContext,
                session,
                ImmutableMap.of(NodeRef.of(analysis.getRecursiveReference(query)), anchorPlan))
                .process(recursionStepRelation, null);

        // coerce recursion step outputs and prune them to contain only the symbols exposed in the scope
        NodeAndMappings coercedRecursionStep;
        List<Type> types = analysis.getRelationCoercion(recursionStepRelation);
        if (types == null) {
            coercedRecursionStep = pruneInvisibleFields(recursionStepPlan, idAllocator);
        }
        else {
            coercedRecursionStep = coerce(recursionStepPlan, types, symbolAllocator, idAllocator);
        }

        NodeAndMappings replacementSpot = new NodeAndMappings(anchorPlan.getRoot(), anchorPlan.getFieldMappings());
        PlanNode recursionStep = coercedRecursionStep.getNode();
        List<Symbol> mappings = coercedRecursionStep.getFields();

        // unroll recursion
        int maxRecursionDepth = getMaxRecursionDepth(session);
        for (int i = 0; i < maxRecursionDepth; i++) {
            recursionSteps.add(copy(recursionStep, mappings));
            NodeAndMappings replacement = copy(recursionStep, mappings);
            recursionStep = replace(recursionStep, replacementSpot, replacement);
            replacementSpot = replacement;
        }

        // after the last recursion step, check if the recursion converged. the last step is expected to return empty result
        // 1. append window to count rows
        NodeAndMappings checkConvergenceStep = copy(recursionStep, mappings);
        Symbol countSymbol = symbolAllocator.newSymbol("count", BIGINT);
        ResolvedFunction function = metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of());
        WindowNode.Frame frame = new WindowNode.Frame(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), Optional.empty(), CURRENT_ROW, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        WindowNode.Function countFunction = new WindowNode.Function(function, ImmutableList.of(), frame, false);

        WindowNode windowNode = new WindowNode(
                idAllocator.getNextId(),
                checkConvergenceStep.getNode(),
                new WindowNode.Specification(ImmutableList.of(), Optional.empty()),
                ImmutableMap.of(countSymbol, countFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        // 2. append filter to fail on non-empty result
        ResolvedFunction fail = metadata.resolveFunction(QualifiedName.of("fail"), fromTypes(VARCHAR));
        String recursionLimitExceededMessage = format("Recursion depth limit exceeded (%s). Use 'max_recursion_depth' session property to modify the limit.", maxRecursionDepth);
        Expression predicate = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN_OR_EQUAL,
                        countSymbol.toSymbolReference(),
                        new GenericLiteral("BIGINT", "0")),
                new Cast(
                        new FunctionCall(
                                fail.toQualifiedName(),
                                ImmutableList.of(new Cast(new StringLiteral(recursionLimitExceededMessage), toSqlType(VARCHAR)))),
                        toSqlType(BOOLEAN)),
                TRUE_LITERAL);
        FilterNode filterNode = new FilterNode(idAllocator.getNextId(), windowNode, predicate);

        recursionSteps.add(new NodeAndMappings(filterNode, checkConvergenceStep.getFields()));

        // union all the recursion steps
        List<NodeAndMappings> recursionStepsToUnion = recursionSteps.build();

        List<Symbol> unionOutputSymbols = anchorPlan.getFieldMappings().stream()
                .map(symbol -> symbolAllocator.newSymbol(symbol, "_expanded"))
                .collect(toImmutableList());

        ImmutableListMultimap.Builder<Symbol, Symbol> unionSymbolMapping = ImmutableListMultimap.builder();
        for (NodeAndMappings plan : recursionStepsToUnion) {
            for (int i = 0; i < unionOutputSymbols.size(); i++) {
                unionSymbolMapping.put(unionOutputSymbols.get(i), plan.getFields().get(i));
            }
        }

        List<PlanNode> nodesToUnion = recursionStepsToUnion.stream()
                .map(NodeAndMappings::getNode)
                .collect(toImmutableList());

        PlanNode result = new UnionNode(idAllocator.getNextId(), nodesToUnion, unionSymbolMapping.build(), unionOutputSymbols);

        if (union.isDistinct()) {
            result = new AggregationNode(
                    idAllocator.getNextId(),
                    result,
                    ImmutableMap.of(),
                    singleGroupingSet(result.getOutputSymbols()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());
        }

        return new RelationPlan(result, anchorPlan.getScope(), unionOutputSymbols, outerContext);
    }

    // Return a copy of the plan and remapped field mappings. In the copied plan:
    // - all PlanNodeIds are replaced with new values,
    // - all symbols are replaced with new symbols.
    // Copying the plan might reorder symbols. The returned field mappings keep the original
    // order and might be used to identify the original output symbols with their copies.
    private NodeAndMappings copy(PlanNode plan, List<Symbol> fields)
    {
        return PlanCopier.copyPlan(plan, fields, metadata, symbolAllocator, idAllocator);
    }

    private PlanNode replace(PlanNode plan, NodeAndMappings replacementSpot, NodeAndMappings replacement)
    {
        checkArgument(
                replacementSpot.getFields().size() == replacement.getFields().size(),
                "mismatching outputs in replacement, expected: %s, got: %s",
                replacementSpot.getFields().size(),
                replacement.getFields().size());

        return SimplePlanRewriter.rewriteWith(new SimplePlanRewriter<Void>()
        {
            @Override
            protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
            {
                return node.replaceChildren(node.getSources().stream()
                        .map(child -> {
                            if (child == replacementSpot.getNode()) {
                                // add projection to adjust symbols
                                Assignments.Builder assignments = Assignments.builder();
                                for (int i = 0; i < replacementSpot.getFields().size(); i++) {
                                    assignments.put(replacementSpot.getFields().get(i), replacement.getFields().get(i).toSymbolReference());
                                }
                                return new ProjectNode(idAllocator.getNextId(), replacement.getNode(), assignments.build());
                            }
                            return context.rewrite(child);
                        })
                        .collect(toImmutableList()));
            }
        }, plan, null);
    }

    public RelationPlan plan(QuerySpecification node)
    {
        PlanBuilder builder = planFrom(node);
        String fromPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(fromPlan, "/tmp/plan/" + id.getAndIncrement() + "-from-plan.dot");

        builder = filter(builder, analysis.getWhere(node), node);
        String filterPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(filterPlan, "/tmp/plan/" + id.getAndIncrement() + "-filter-plan.dot");

        builder = aggregate(builder, node);
        String aggPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(aggPlan, "/tmp/plan/" + id.getAndIncrement() + "-agg-plan.dot");

        builder = filter(builder, analysis.getHaving(node), node);
        String havePlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(havePlan, "/tmp/plan/" + id.getAndIncrement() + "-having-plan.dot");

        builder = window(node, builder, ImmutableList.copyOf(analysis.getWindowFunctions(node)));
        String windowPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(windowPlan, "/tmp/plan/" + id.getAndIncrement() + "-window-plan.dot");

        List<SelectExpression> selectExpressions = analysis.getSelectExpressions(node);
        List<Expression> expressions = selectExpressions.stream()
                .map(SelectExpression::getExpression)
                .collect(toImmutableList());
        builder = subqueryPlanner.handleSubqueries(builder, expressions, node);
        String selectPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(selectPlan, "/tmp/plan/" + id.getAndIncrement() + "-select-plan.dot");

        if (hasExpressionsToUnfold(selectExpressions)) {
            // pre-project the folded expressions to preserve any non-deterministic semantics of functions that might be referenced
            builder = builder.appendProjections(expressions, symbolAllocator, idAllocator);
        }

        List<Expression> outputs = outputExpressions(selectExpressions);
        if (node.getOrderBy().isPresent()) {
            // ORDER BY requires outputs of SELECT to be visible.
            // For queries with aggregation, it also requires grouping keys and translated aggregations.
            if (analysis.isAggregation(node)) {
                // Add projections for aggregations required by ORDER BY. After this step, grouping keys and translated
                // aggregations are visible.
                List<Expression> orderByAggregates = analysis.getOrderByAggregates(node.getOrderBy().get());
                builder = builder.appendProjections(orderByAggregates, symbolAllocator, idAllocator);
            }

            // Add projections for the outputs of SELECT, but stack them on top of the ones from the FROM clause so both are visible
            // when resolving the ORDER BY clause.
            builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

            // The new scope is the composite of the fields from the FROM and SELECT clause (local nested scopes). Fields from the bottom of
            // the scope stack need to be placed first to match the expected layout for nested scopes.
            List<Symbol> newFields = new ArrayList<>();
            newFields.addAll(builder.getTranslations().getFieldSymbols());

            outputs.stream()
                    .map(builder::translate)
                    .forEach(newFields::add);

            builder = builder.withScope(analysis.getScope(node.getOrderBy().get()), newFields);

            builder = window(node, builder, ImmutableList.copyOf(analysis.getOrderByWindowFunctions(node.getOrderBy().get())));

            String orderPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
            PlanPrinter.printGraphviz(orderPlan, "/tmp/plan/" + id.getAndIncrement() + "-orderby-plan.dot");
        }

        List<Expression> orderBy = analysis.getOrderByExpressions(node);
        builder = subqueryPlanner.handleSubqueries(builder, orderBy, node);

        String orderSubuqeryPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(orderSubuqeryPlan, "/tmp/plan/" + id.getAndIncrement() + "-orderby-subquery-plan.dot");

        builder = builder.appendProjections(Iterables.concat(orderBy, outputs), symbolAllocator, idAllocator);
        String projectPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(projectPlan, "/tmp/plan/" + id.getAndIncrement() + "-project-plan.dot");

        builder = distinct(builder, node, outputs);
        String distinctPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(distinctPlan, "/tmp/plan/" + id.getAndIncrement() + "-distinct-plan.dot");

        Optional<OrderingScheme> orderingScheme = orderingScheme(builder, node.getOrderBy(), analysis.getOrderByExpressions(node));
        builder = sort(builder, orderingScheme);

        String sortPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(sortPlan, "/tmp/plan/" + id.getAndIncrement() + "-sort-plan.dot");

        builder = offset(builder, node.getOffset());

        String offsetPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(offsetPlan, "/tmp/plan/" + id.getAndIncrement() + "-offset-plan.dot");

        builder = limit(builder, node.getLimit(), orderingScheme);
        String limitPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(limitPlan, "/tmp/plan/" + id.getAndIncrement() + "-limit-plan.dot");

        builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);
        String outputPlan = PlanPrinter.graphvizLogicalPlan(builder.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(outputPlan, "/tmp/plan/" + id.getAndIncrement() + "-output-plan.dot");

        return new RelationPlan(
                builder.getRoot(),
                analysis.getScope(node),
                computeOutputs(builder, outputs),
                outerContext);
    }

    private boolean hasExpressionsToUnfold(List<SelectExpression> selectExpressions)
    {
        return selectExpressions.stream()
                .map(SelectExpression::getUnfoldedExpressions)
                .anyMatch(Optional::isPresent);
    }

    private List<Expression> outputExpressions(List<SelectExpression> selectExpressions)
    {
        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (SelectExpression selectExpression : selectExpressions) {
            if (selectExpression.getUnfoldedExpressions().isPresent()) {
                result.addAll(selectExpression.getUnfoldedExpressions().get());
            }
            else {
                result.add(selectExpression.getExpression());
            }
        }
        return result.build();
    }

    public DeleteNode plan(Delete node)
    {
        Table table = node.getTable();
        TableHandle handle = analysis.getTableHandle(table);

        // create table scan
        RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, outerContext, session, recursiveSubqueries)
                .process(table, null);

        PlanBuilder builder = newPlanBuilder(relationPlan, analysis, lambdaDeclarationToSymbolMap);
        if (node.getWhere().isPresent()) {
            builder = filter(builder, node.getWhere().get(), node);
        }

        // create delete node
        Symbol rowId = builder.translate(analysis.getRowIdField(table));
        List<Symbol> outputs = ImmutableList.of(
                symbolAllocator.newSymbol("partialrows", BIGINT),
                symbolAllocator.newSymbol("fragment", VARBINARY));

        return new DeleteNode(idAllocator.getNextId(), builder.getRoot(), new DeleteTarget(handle, metadata.getTableMetadata(session, handle).getTable()), rowId, outputs);
    }

    private static List<Symbol> computeOutputs(PlanBuilder builder, List<Expression> outputExpressions)
    {
        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (Expression expression : outputExpressions) {
            outputSymbols.add(builder.translate(expression));
        }
        return outputSymbols.build();
    }

    private PlanBuilder planQueryBody(Query query)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, outerContext, session, recursiveSubqueries)
                .process(query.getQueryBody(), null);

        return newPlanBuilder(relationPlan, analysis, lambdaDeclarationToSymbolMap);
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        if (node.getFrom().isPresent()) {
            RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, outerContext, session, recursiveSubqueries)
                    .process(node.getFrom().get(), null);
            return newPlanBuilder(relationPlan, analysis, lambdaDeclarationToSymbolMap);
        }

        return new PlanBuilder(
                new TranslationMap(outerContext, analysis.getImplicitFromScope(node), analysis, lambdaDeclarationToSymbolMap, ImmutableList.of()),
                new ValuesNode(idAllocator.getNextId(), 1));
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node)
    {
        if (predicate == null) {
            return subPlan;
        }

        subPlan = subqueryPlanner.handleSubqueries(subPlan, predicate, node);

        PlanBuilder newfilter = subPlan.withNewRoot(new FilterNode(idAllocator.getNextId(), subPlan.getRoot(), subPlan.rewrite(predicate)));

        String selectPlan = PlanPrinter.graphvizLogicalPlan(newfilter.getRoot(), symbolAllocator.getTypes());
        PlanPrinter.printGraphviz(selectPlan, "/tmp/plan/" + id.getAndIncrement() + "-newfilter-plan.dot");

        return newfilter;
    }

    private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node)
    {
        if (!analysis.isAggregation(node)) {
            return subPlan;
        }

        ImmutableList.Builder<Expression> inputBuilder = ImmutableList.builder();
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getArguments)
                .flatMap(List::stream)
                .filter(expression -> !(expression instanceof LambdaExpression)) // lambda expression is generated at execution time
                .forEach(inputBuilder::add);

        analysis.getAggregates(node).stream()
                .map(FunctionCall::getOrderBy)
                .map(NodeUtils::getSortItemsFromOrderBy)
                .flatMap(List::stream)
                .map(SortItem::getSortKey)
                .forEach(inputBuilder::add);

        // filter expressions need to be projected first
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getFilter)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(inputBuilder::add);

        GroupingSetAnalysis groupingSetAnalysis = analysis.getGroupingSets(node);
        inputBuilder.addAll(groupingSetAnalysis.getComplexExpressions());

        List<Expression> inputs = inputBuilder.build();
        subPlan = subqueryPlanner.handleSubqueries(subPlan, inputs, node);
        subPlan = subPlan.appendProjections(inputs, symbolAllocator, idAllocator);

        // Add projection to coerce inputs to their site-specific types.
        // This is important because the same lexical expression may need to be coerced
        // in different ways if it's referenced by multiple arguments to the window function.
        // For example, given v::integer,
        //    avg(v)
        // Needs to be rewritten as
        //    avg(CAST(v AS double))
        PlanAndMappings coercions = coerce(subPlan, inputs, analysis, idAllocator, symbolAllocator, typeCoercion);
        subPlan = coercions.getSubPlan();

        GroupingSetsPlan groupingSets = planGroupingSets(subPlan, node, groupingSetAnalysis);

        subPlan = planAggregation(groupingSets.getSubPlan(), groupingSets.getGroupingSets(), groupingSets.getGroupIdSymbol(), analysis.getAggregates(node), coercions::get);

        return planGroupingOperations(subPlan, node, groupingSets.getGroupIdSymbol(), groupingSets.getColumnOnlyGroupingSets());
    }

    private GroupingSetsPlan planGroupingSets(PlanBuilder subPlan, QuerySpecification node, GroupingSetAnalysis groupingSetAnalysis)
    {
        Map<Symbol, Symbol> groupingSetMappings = new LinkedHashMap<>();

        // Compute a set of artificial columns that will contain the values of the original columns
        // filtered by whether the column is included in the grouping set
        // This will become the basis for the scope for any column references
        Symbol[] fields = new Symbol[subPlan.getTranslations().getFieldSymbols().size()];
        for (FieldId field : groupingSetAnalysis.getAllFields()) {
            Symbol input = subPlan.getTranslations().getFieldSymbols().get(field.getFieldIndex());
            Symbol output = symbolAllocator.newSymbol(input, "gid");
            fields[field.getFieldIndex()] = output;
            groupingSetMappings.put(output, input);
        }

        Map<ScopeAware<Expression>, Symbol> complexExpressions = new HashMap<>();
        for (Expression expression : groupingSetAnalysis.getComplexExpressions()) {
            if (!complexExpressions.containsKey(scopeAwareKey(expression, analysis, subPlan.getScope()))) {
                Symbol input = subPlan.translate(expression);
                Symbol output = symbolAllocator.newSymbol(expression, analysis.getType(expression), "gid");
                complexExpressions.put(scopeAwareKey(expression, analysis, subPlan.getScope()), output);
                groupingSetMappings.put(output, input);
            }
        }

        // For the purpose of "distinct", we need to canonicalize column references that may have varying
        // syntactic forms (e.g., "t.a" vs "a"). Thus we need to enumerate grouping sets based on the underlying
        // fieldId associated with each column reference expression.

        // The catch is that simple group-by expressions can be arbitrary expressions (this is a departure from the SQL specification).
        // But, they don't affect the number of grouping sets or the behavior of "distinct" . We can compute all the candidate
        // grouping sets in terms of fieldId, dedup as appropriate and then cross-join them with the complex expressions.

        // This tracks the grouping sets before complex expressions are considered.
        // It's also used to compute the descriptors needed to implement grouping()
        List<Set<FieldId>> columnOnlyGroupingSets = enumerateGroupingSets(groupingSetAnalysis);
        if (node.getGroupBy().isPresent() && node.getGroupBy().get().isDistinct()) {
            columnOnlyGroupingSets = columnOnlyGroupingSets.stream()
                    .distinct()
                    .collect(toImmutableList());
        }

        // translate from FieldIds to Symbols
        List<List<Symbol>> sets = columnOnlyGroupingSets.stream()
                .map(set -> set.stream()
                        .map(FieldId::getFieldIndex)
                        .map(index -> fields[index])
                        .collect(toImmutableList()))
                .collect(toImmutableList());

        // combine (cartesian product) with complex expressions
        List<List<Symbol>> groupingSets = sets.stream()
                .map(set -> ImmutableList.<Symbol>builder()
                        .addAll(set)
                        .addAll(complexExpressions.values())
                        .build())
                .collect(toImmutableList());

        // Generate GroupIdNode (multiple grouping sets) or ProjectNode (single grouping set)
        PlanNode groupId;
        Optional<Symbol> groupIdSymbol = Optional.empty();
        if (groupingSets.size() > 1) {
            groupIdSymbol = Optional.of(symbolAllocator.newSymbol("groupId", BIGINT));
            groupId = new GroupIdNode(
                    idAllocator.getNextId(),
                    subPlan.getRoot(),
                    groupingSets,
                    groupingSetMappings,
                    subPlan.getRoot().getOutputSymbols(),
                    groupIdSymbol.get());
        }
        else {
            Assignments.Builder assignments = Assignments.builder();
            assignments.putIdentities(subPlan.getRoot().getOutputSymbols());
            groupingSetMappings.forEach((key, value) -> assignments.put(key, value.toSymbolReference()));

            groupId = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build());
        }

        subPlan = new PlanBuilder(
                subPlan.getTranslations()
                        .withNewMappings(complexExpressions, Arrays.asList(fields)),
                groupId);

        return new GroupingSetsPlan(subPlan, columnOnlyGroupingSets, groupingSets, groupIdSymbol);
    }

    private PlanBuilder planAggregation(PlanBuilder subPlan, List<List<Symbol>> groupingSets, Optional<Symbol> groupIdSymbol, List<FunctionCall> aggregates, Function<Expression, Symbol> coercions)
    {
        ImmutableList.Builder<AggregationAssignment> aggregateMappingBuilder = ImmutableList.builder();

        // deduplicate based on scope-aware equality
        for (FunctionCall function : scopeAwareDistinct(subPlan, aggregates)) {
            Symbol symbol = symbolAllocator.newSymbol(function, analysis.getType(function));

            // TODO: for ORDER BY arguments, rewrite them such that they match the actual arguments to the function. This is necessary to maintain the semantics of DISTINCT + ORDER BY,
            //   which requires that ORDER BY be a subset of arguments
            //   What can happen currently is that if the argument requires a coercion, the argument will take a different input that the ORDER BY clause, which is undefined behavior
            Aggregation aggregation = new Aggregation(
                    analysis.getResolvedFunction(function),
                    function.getArguments().stream()
                            .map(argument -> {
                                if (argument instanceof LambdaExpression) {
                                    return subPlan.rewrite(argument);
                                }
                                return coercions.apply(argument).toSymbolReference();
                            })
                            .collect(toImmutableList()),
                    function.isDistinct(),
                    function.getFilter().map(coercions),
                    function.getOrderBy().map(orderBy -> translateOrderingScheme(orderBy.getSortItems(), coercions)),
                    Optional.empty());

            aggregateMappingBuilder.add(new AggregationAssignment(symbol, function, aggregation));
        }
        List<AggregationAssignment> aggregateMappings = aggregateMappingBuilder.build();

        ImmutableSet.Builder<Integer> globalGroupingSets = ImmutableSet.builder();
        for (int i = 0; i < groupingSets.size(); i++) {
            if (groupingSets.get(i).isEmpty()) {
                globalGroupingSets.add(i);
            }
        }

        ImmutableList.Builder<Symbol> groupingKeys = ImmutableList.builder();
        groupingSets.stream()
                .flatMap(List::stream)
                .distinct()
                .forEach(groupingKeys::add);
        groupIdSymbol.ifPresent(groupingKeys::add);

        AggregationNode aggregationNode = new AggregationNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                aggregateMappings.stream()
                        .collect(toImmutableMap(AggregationAssignment::getSymbol, AggregationAssignment::getRewritten)),
                groupingSets(
                        groupingKeys.build(),
                        groupingSets.size(),
                        globalGroupingSets.build()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                groupIdSymbol);

        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(aggregateMappings.stream()
                                .collect(toImmutableMap(assignment -> scopeAwareKey(assignment.getAstExpression(), analysis, subPlan.getScope()), AggregationAssignment::getSymbol))),
                aggregationNode);
    }

    private <T extends Expression> List<T> scopeAwareDistinct(PlanBuilder subPlan, List<T> expressions)
    {
        return expressions.stream()
                .map(function -> scopeAwareKey(function, analysis, subPlan.getScope()))
                .distinct()
                .map(ScopeAware::getNode)
                .collect(toImmutableList());
    }

    private OrderingScheme translateOrderingScheme(List<SortItem> items, Function<Expression, Symbol> coercions)
    {
        List<Symbol> symbols = items.stream()
                .map(SortItem::getSortKey)
                .map(coercions)
                .collect(toImmutableList());

        ImmutableMap.Builder<Symbol, SortOrder> orders = ImmutableMap.builder();
        for (int i = 0; i < symbols.size(); i++) {
            orders.put(symbols.get(i), OrderingScheme.sortItemToSortOrder(items.get(i)));
        }

        return new OrderingScheme(symbols, orders.build());
    }

    private List<Set<FieldId>> enumerateGroupingSets(GroupingSetAnalysis groupingSetAnalysis)
    {
        List<List<Set<FieldId>>> partialSets = new ArrayList<>();

        for (Set<FieldId> cube : groupingSetAnalysis.getCubes()) {
            partialSets.add(ImmutableList.copyOf(Sets.powerSet(cube)));
        }

        for (List<FieldId> rollup : groupingSetAnalysis.getRollups()) {
            List<Set<FieldId>> sets = IntStream.rangeClosed(0, rollup.size())
                    .mapToObj(i -> ImmutableSet.copyOf(rollup.subList(0, i)))
                    .collect(toImmutableList());

            partialSets.add(sets);
        }

        partialSets.addAll(groupingSetAnalysis.getOrdinarySets());

        if (partialSets.isEmpty()) {
            return ImmutableList.of(ImmutableSet.of());
        }

        // compute the cross product of the partial sets
        List<Set<FieldId>> allSets = new ArrayList<>();
        partialSets.get(0)
                .stream()
                .map(ImmutableSet::copyOf)
                .forEach(allSets::add);

        for (int i = 1; i < partialSets.size(); i++) {
            List<Set<FieldId>> groupingSets = partialSets.get(i);
            List<Set<FieldId>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(allSets);
            allSets.clear();
            for (Set<FieldId> existingSet : oldGroupingSetsCrossProduct) {
                for (Set<FieldId> groupingSet : groupingSets) {
                    Set<FieldId> concatenatedSet = ImmutableSet.<FieldId>builder()
                            .addAll(existingSet)
                            .addAll(groupingSet)
                            .build();
                    allSets.add(concatenatedSet);
                }
            }
        }

        return allSets;
    }

    private PlanBuilder planGroupingOperations(PlanBuilder subPlan, QuerySpecification node, Optional<Symbol> groupIdSymbol, List<Set<FieldId>> groupingSets)
    {
        if (analysis.getGroupingOperations(node).isEmpty()) {
            return subPlan;
        }

        List<Set<Integer>> descriptor = groupingSets.stream()
                .map(set -> set.stream()
                        .map(FieldId::getFieldIndex)
                        .collect(toImmutableSet()))
                .collect(toImmutableList());

        return subPlan.appendProjections(
                analysis.getGroupingOperations(node),
                symbolAllocator,
                idAllocator,
                (translations, groupingOperation) -> rewriteGroupingOperation(groupingOperation, descriptor, analysis.getColumnReferenceFields(), groupIdSymbol),
                (translations, groupingOperation) -> false);
    }

    private PlanBuilder window(Node node, PlanBuilder subPlan, List<FunctionCall> windowFunctions)
    {
        if (windowFunctions.isEmpty()) {
            return subPlan;
        }

        for (FunctionCall windowFunction : scopeAwareDistinct(subPlan, windowFunctions)) {
            checkArgument(windowFunction.getFilter().isEmpty(), "Window functions cannot have filter");

            ResolvedWindow window = analysis.getWindow(windowFunction);
            checkState(window != null, "no resolved window for: " + windowFunction);

            // Pre-project inputs.
            // Predefined window parts (specified in WINDOW clause) can only use source symbols, and no output symbols.
            // It matters in case when this window planning takes place in ORDER BY clause, where both source and output
            // symbols are visible.
            // This issue is solved by analyzing window definitions in the source scope. After analysis, the expressions
            // are recorded as belonging to the source scope, and consequentially source symbols will be used to plan them.
            ImmutableList.Builder<Expression> inputsBuilder = ImmutableList.<Expression>builder()
                    .addAll(windowFunction.getArguments().stream()
                            .filter(argument -> !(argument instanceof LambdaExpression)) // lambda expression is generated at execution time
                            .collect(Collectors.toList()))
                    .addAll(window.getPartitionBy())
                    .addAll(getSortItemsFromOrderBy(window.getOrderBy()).stream()
                            .map(SortItem::getSortKey)
                            .iterator());

            if (window.getFrame().isPresent()) {
                WindowFrame frame = window.getFrame().get();
                frame.getStart().getValue().ifPresent(inputsBuilder::add);

                if (frame.getEnd().isPresent()) {
                    frame.getEnd().get().getValue().ifPresent(inputsBuilder::add);
                }
            }

            List<Expression> inputs = inputsBuilder.build();

            subPlan = subqueryPlanner.handleSubqueries(subPlan, inputs, node);
            subPlan = subPlan.appendProjections(inputs, symbolAllocator, idAllocator);

            // Add projection to coerce inputs to their site-specific types.
            // This is important because the same lexical expression may need to be coerced
            // in different ways if it's referenced by multiple arguments to the window function.
            // For example, given v::integer,
            //    avg(v) OVER (ORDER BY v)
            // Needs to be rewritten as
            //    avg(CAST(v AS double)) OVER (ORDER BY v)
            PlanAndMappings coercions = coerce(subPlan, inputs, analysis, idAllocator, symbolAllocator, typeCoercion);
            subPlan = coercions.getSubPlan();

            // For frame of type RANGE, append casts and functions necessary for frame bound calculations
            Optional<Symbol> frameStart = Optional.empty();
            Optional<Symbol> frameEnd = Optional.empty();
            Optional<Symbol> sortKeyCoercedForFrameStartComparison = Optional.empty();
            Optional<Symbol> sortKeyCoercedForFrameEndComparison = Optional.empty();

            if (window.getFrame().isPresent() && window.getFrame().get().getType() == RANGE) {
                Optional<Expression> startValue = window.getFrame().get().getStart().getValue();
                Optional<Expression> endValue = window.getFrame().get().getEnd().flatMap(FrameBound::getValue);
                // record sortKey coercions for reuse
                Map<Type, Symbol> sortKeyCoercions = new HashMap<>();

                // process frame start
                FrameBoundPlanAndSymbols plan = planFrameBound(subPlan, coercions, startValue, window, sortKeyCoercions);
                subPlan = plan.getSubPlan();
                frameStart = plan.getFrameBoundSymbol();
                sortKeyCoercedForFrameStartComparison = plan.getSortKeyCoercedForFrameBoundComparison();

                // process frame end
                plan = planFrameBound(subPlan, coercions, endValue, window, sortKeyCoercions);
                subPlan = plan.getSubPlan();
                frameEnd = plan.getFrameBoundSymbol();
                sortKeyCoercedForFrameEndComparison = plan.getSortKeyCoercedForFrameBoundComparison();
            }
            else if (window.getFrame().isPresent() && (window.getFrame().get().getType() == ROWS || window.getFrame().get().getType() == GROUPS)) {
                frameStart = window.getFrame().get().getStart().getValue().map(coercions::get);
                frameEnd = window.getFrame().get().getEnd().flatMap(FrameBound::getValue).map(coercions::get);
            }
            else if (window.getFrame().isPresent()) {
                throw new IllegalArgumentException("unexpected window frame type: " + window.getFrame().get().getType());
            }

            subPlan = planWindow(subPlan, windowFunction, window, coercions, frameStart, sortKeyCoercedForFrameStartComparison, frameEnd, sortKeyCoercedForFrameEndComparison);
        }

        return subPlan;
    }

    private FrameBoundPlanAndSymbols planFrameBound(PlanBuilder subPlan, PlanAndMappings coercions, Optional<Expression> frameOffset, ResolvedWindow window, Map<Type, Symbol> sortKeyCoercions)
    {
        Optional<ResolvedFunction> frameBoundCalculationFunction = frameOffset.map(analysis::getFrameBoundCalculation);

        // Empty frameBoundCalculationFunction indicates that frame bound type is CURRENT ROW or UNBOUNDED.
        // Handling it doesn't require any additional symbols.
        if (frameBoundCalculationFunction.isEmpty()) {
            return new FrameBoundPlanAndSymbols(subPlan, Optional.empty(), Optional.empty());
        }

        // Present frameBoundCalculationFunction indicates that frame bound type is <expression> PRECEDING or <expression> FOLLOWING.
        // It requires adding certain projections to the plan so that the operator can determine frame bounds.

        // First, append filter to validate offset values. They mustn't be negative or null.
        Symbol offsetSymbol = coercions.get(frameOffset.get());
        Expression zeroOffset = zeroOfType(symbolAllocator.getTypes().get(offsetSymbol));
        ResolvedFunction fail = metadata.resolveFunction(QualifiedName.of("fail"), fromTypes(VARCHAR));
        Expression predicate = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN_OR_EQUAL,
                        offsetSymbol.toSymbolReference(),
                        zeroOffset),
                TRUE_LITERAL,
                new Cast(
                        new FunctionCall(
                                fail.toQualifiedName(),
                                ImmutableList.of(new Cast(new StringLiteral("Window frame offset value must not be negative or null"), toSqlType(VARCHAR)))),
                        toSqlType(BOOLEAN)));
        subPlan = subPlan.withNewRoot(new FilterNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                predicate));

        // Then, coerce the sortKey so that we can add / subtract the offset.
        // Note: for that we cannot rely on the usual mechanism of using the coerce() method. The coerce() method can only handle one coercion for a node,
        // while the sortKey node might require several different coercions, e.g. one for frame start and one for frame end.
        Expression sortKey = Iterables.getOnlyElement(window.getOrderBy().get().getSortItems()).getSortKey();
        Symbol sortKeyCoercedForFrameBoundCalculation = coercions.get(sortKey);
        Optional<Type> coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundCalculation);
        if (coercion.isPresent()) {
            Type expectedType = coercion.get();
            Symbol alreadyCoerced = sortKeyCoercions.get(expectedType);
            if (alreadyCoerced != null) {
                sortKeyCoercedForFrameBoundCalculation = alreadyCoerced;
            }
            else {
                Expression cast = new Cast(
                        coercions.get(sortKey).toSymbolReference(),
                        toSqlType(expectedType),
                        false,
                        typeCoercion.isTypeOnlyCoercion(analysis.getType(sortKey), expectedType));
                sortKeyCoercedForFrameBoundCalculation = symbolAllocator.newSymbol(cast, expectedType);
                sortKeyCoercions.put(expectedType, sortKeyCoercedForFrameBoundCalculation);
                subPlan = subPlan.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putIdentities(subPlan.getRoot().getOutputSymbols())
                                .put(sortKeyCoercedForFrameBoundCalculation, cast)
                                .build()));
            }
        }

        // Next, pre-project the function which combines sortKey with the offset.
        // Note: if frameOffset needs a coercion, it was added before by a call to coerce() method.
        ResolvedFunction function = frameBoundCalculationFunction.get();
        Expression functionCall = new FunctionCall(
                function.toQualifiedName(),
                ImmutableList.of(
                        sortKeyCoercedForFrameBoundCalculation.toSymbolReference(),
                        offsetSymbol.toSymbolReference()));
        Symbol frameBoundSymbol = symbolAllocator.newSymbol(functionCall, function.getSignature().getReturnType());
        subPlan = subPlan.withNewRoot(new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                Assignments.builder()
                        .putIdentities(subPlan.getRoot().getOutputSymbols())
                        .put(frameBoundSymbol, functionCall)
                        .build()));

        // Finally, coerce the sortKey to the type of frameBound so that the operator can perform comparisons on them
        Optional<Symbol> sortKeyCoercedForFrameBoundComparison = Optional.of(coercions.get(sortKey));
        coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundComparison);
        if (coercion.isPresent()) {
            Type expectedType = coercion.get();
            Symbol alreadyCoerced = sortKeyCoercions.get(expectedType);
            if (alreadyCoerced != null) {
                sortKeyCoercedForFrameBoundComparison = Optional.of(alreadyCoerced);
            }
            else {
                Expression cast = new Cast(
                        coercions.get(sortKey).toSymbolReference(),
                        toSqlType(expectedType),
                        false,
                        typeCoercion.isTypeOnlyCoercion(analysis.getType(sortKey), expectedType));
                Symbol castSymbol = symbolAllocator.newSymbol(cast, expectedType);
                sortKeyCoercions.put(expectedType, castSymbol);
                subPlan = subPlan.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putIdentities(subPlan.getRoot().getOutputSymbols())
                                .put(castSymbol, cast)
                                .build()));
                sortKeyCoercedForFrameBoundComparison = Optional.of(castSymbol);
            }
        }

        return new FrameBoundPlanAndSymbols(subPlan, Optional.of(frameBoundSymbol), sortKeyCoercedForFrameBoundComparison);
    }

    private Expression zeroOfType(Type type)
    {
        if (isNumericType(type)) {
            return new Cast(new LongLiteral("0"), toSqlType(type));
        }
        if (type.equals(INTERVAL_DAY_TIME)) {
            return new IntervalLiteral("0", POSITIVE, DAY);
        }
        if (type.equals(INTERVAL_YEAR_MONTH)) {
            return new IntervalLiteral("0", POSITIVE, YEAR);
        }
        throw new IllegalArgumentException("unexpected type: " + type);
    }

    private PlanBuilder planWindow(
            PlanBuilder subPlan,
            FunctionCall windowFunction,
            ResolvedWindow window,
            PlanAndMappings coercions,
            Optional<Symbol> frameStartSymbol,
            Optional<Symbol> sortKeyCoercedForFrameStartComparison,
            Optional<Symbol> frameEndSymbol,
            Optional<Symbol> sortKeyCoercedForFrameEndComparison)
    {
        WindowFrame.Type frameType = WindowFrame.Type.RANGE;
        FrameBound.Type frameStartType = FrameBound.Type.UNBOUNDED_PRECEDING;
        FrameBound.Type frameEndType = FrameBound.Type.CURRENT_ROW;

        Optional<Expression> frameStartExpression = Optional.empty();
        Optional<Expression> frameEndExpression = Optional.empty();

        if (window.getFrame().isPresent()) {
            WindowFrame frame = window.getFrame().get();
            frameType = frame.getType();

            frameStartType = frame.getStart().getType();
            frameStartExpression = frame.getStart().getValue();

            if (frame.getEnd().isPresent()) {
                frameEndType = frame.getEnd().get().getType();
                frameEndExpression = frame.getEnd().get().getValue();
            }
        }

        // Rewrite PARTITION BY in terms of pre-projected inputs
        ImmutableList.Builder<Symbol> partitionBySymbols = ImmutableList.builder();
        for (Expression expression : window.getPartitionBy()) {
            partitionBySymbols.add(coercions.get(expression));
        }

        // Rewrite ORDER BY in terms of pre-projected inputs
        LinkedHashMap<Symbol, SortOrder> orderings = new LinkedHashMap<>();
        for (SortItem item : getSortItemsFromOrderBy(window.getOrderBy())) {
            Symbol symbol = coercions.get(item.getSortKey());
            // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
            orderings.putIfAbsent(symbol, sortItemToSortOrder(item));
        }

        // Rewrite frame bounds in terms of pre-projected inputs
        WindowNode.Frame frame = new WindowNode.Frame(
                frameType,
                frameStartType,
                frameStartSymbol,
                sortKeyCoercedForFrameStartComparison,
                frameEndType,
                frameEndSymbol,
                sortKeyCoercedForFrameEndComparison,
                frameStartExpression,
                frameEndExpression);

        Symbol newSymbol = symbolAllocator.newSymbol(windowFunction, analysis.getType(windowFunction));

        NullTreatment nullTreatment = windowFunction.getNullTreatment()
                .orElse(NullTreatment.RESPECT);

        WindowNode.Function function = new WindowNode.Function(
                analysis.getResolvedFunction(windowFunction),
                windowFunction.getArguments().stream()
                        .map(argument -> {
                            if (argument instanceof LambdaExpression) {
                                return subPlan.rewrite(argument);
                            }
                            return coercions.get(argument).toSymbolReference();
                        })
                        .collect(toImmutableList()),
                frame,
                nullTreatment == NullTreatment.IGNORE);

        ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
        orderBySymbols.addAll(orderings.keySet());
        Optional<OrderingScheme> orderingScheme = Optional.empty();
        if (!orderings.isEmpty()) {
            orderingScheme = Optional.of(new OrderingScheme(orderBySymbols.build(), orderings));
        }

        // create window node
        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(ImmutableMap.of(scopeAwareKey(windowFunction, analysis, subPlan.getScope()), newSymbol)),
                new WindowNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        new WindowNode.Specification(
                                partitionBySymbols.build(),
                                orderingScheme),
                        ImmutableMap.of(newSymbol, function),
                        Optional.empty(),
                        ImmutableSet.of(),
                        0));
    }

    /**
     * Creates a projection with any additional coercions by identity of the provided expressions.
     *
     * @return the new subplan and a mapping of each expression to the symbol representing the coercion or an existing symbol if a coercion wasn't needed
     */
    public static PlanAndMappings coerce(PlanBuilder subPlan, List<Expression> expressions, Analysis analysis, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, TypeCoercion typeCoercion)
    {
        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(subPlan.getRoot().getOutputSymbols());

        ImmutableMap.Builder<NodeRef<Expression>, Symbol> mappings = ImmutableMap.builder();
        for (Expression expression : expressions) {
            Type coercion = analysis.getCoercion(expression);

            if (coercion != null) {
                Type type = analysis.getType(expression);
                Symbol symbol = symbolAllocator.newSymbol(expression, coercion);

                assignments.put(symbol, new Cast(
                        subPlan.rewrite(expression),
                        toSqlType(coercion),
                        false,
                        typeCoercion.isTypeOnlyCoercion(type, coercion)));

                mappings.put(NodeRef.of(expression), symbol);
            }
            else {
                mappings.put(NodeRef.of(expression), subPlan.translate(expression));
            }
        }

        subPlan = subPlan.withNewRoot(
                new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        assignments.build()));

        return new PlanAndMappings(subPlan, mappings.build());
    }

    public static Expression coerceIfNecessary(Analysis analysis, Expression original, Expression rewritten)
    {
        Type coercion = analysis.getCoercion(original);
        if (coercion == null) {
            return rewritten;
        }

        return new Cast(
                rewritten,
                toSqlType(coercion),
                false,
                analysis.isTypeOnlyCoercion(original));
    }

    public static NodeAndMappings coerce(RelationPlan plan, List<Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        List<Symbol> visibleFields = visibleFields(plan);
        checkArgument(visibleFields.size() == types.size());

        Assignments.Builder assignments = Assignments.builder();
        ImmutableList.Builder<Symbol> mappings = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            Symbol input = visibleFields.get(i);
            Type type = types.get(i);

            if (!symbolAllocator.getTypes().get(input).equals(type)) {
                Symbol coerced = symbolAllocator.newSymbol(input.getName(), type);
                assignments.put(coerced, new Cast(input.toSymbolReference(), toSqlType(type)));
                mappings.add(coerced);
            }
            else {
                assignments.putIdentity(input);
                mappings.add(input);
            }
        }

        ProjectNode coerced = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        return new NodeAndMappings(coerced, mappings.build());
    }

    private static List<Symbol> visibleFields(RelationPlan subPlan)
    {
        RelationType descriptor = subPlan.getDescriptor();
        return descriptor.getAllFields().stream()
                .filter(field -> !field.isHidden())
                .map(descriptor::indexOf)
                .map(subPlan.getFieldMappings()::get)
                .collect(toImmutableList());
    }

    public static NodeAndMappings pruneInvisibleFields(RelationPlan plan, PlanNodeIdAllocator idAllocator)
    {
        List<Symbol> visibleFields = visibleFields(plan);
        ProjectNode pruned = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), Assignments.identity(visibleFields));
        return new NodeAndMappings(pruned, visibleFields);
    }

    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node, List<Expression> expressions)
    {
        if (node.getSelect().isDistinct()) {
            List<Symbol> symbols = expressions.stream()
                    .map(subPlan::translate)
                    .collect(Collectors.toList());

            return subPlan.withNewRoot(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            ImmutableMap.of(),
                            singleGroupingSet(symbols),
                            ImmutableList.of(),
                            AggregationNode.Step.SINGLE,
                            Optional.empty(),
                            Optional.empty()));
        }

        return subPlan;
    }

    private Optional<OrderingScheme> orderingScheme(PlanBuilder subPlan, Optional<OrderBy> orderBy, List<Expression> orderByExpressions)
    {
        if (orderBy.isEmpty() || (isSkipRedundantSort(session)) && analysis.isOrderByRedundant(orderBy.get())) {
            return Optional.empty();
        }

        Iterator<SortItem> sortItems = orderBy.get().getSortItems().iterator();

        ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
        Map<Symbol, SortOrder> orderings = new HashMap<>();
        for (Expression fieldOrExpression : orderByExpressions) {
            Symbol symbol = subPlan.translate(fieldOrExpression);

            SortItem sortItem = sortItems.next();
            if (!orderings.containsKey(symbol)) {
                orderBySymbols.add(symbol);
                orderings.put(symbol, sortItemToSortOrder(sortItem));
            }
        }
        return Optional.of(new OrderingScheme(orderBySymbols.build(), orderings));
    }

    private PlanBuilder sort(PlanBuilder subPlan, Optional<OrderingScheme> orderingScheme)
    {
        if (orderingScheme.isEmpty()) {
            return subPlan;
        }

        return subPlan.withNewRoot(
                new SortNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        orderingScheme.get(),
                        false));
    }

    private PlanBuilder offset(PlanBuilder subPlan, Optional<Offset> offset)
    {
        if (offset.isEmpty()) {
            return subPlan;
        }

        return subPlan.withNewRoot(
                new OffsetNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        analysis.getOffset(offset.get())));
    }

    private PlanBuilder limit(PlanBuilder subPlan, Optional<Node> limit, Optional<OrderingScheme> orderingScheme)
    {
        if (limit.isPresent() && analysis.getLimit(limit.get()).isPresent()) {
            Optional<OrderingScheme> tiesResolvingScheme = Optional.empty();
            if (limit.get() instanceof FetchFirst && ((FetchFirst) limit.get()).isWithTies()) {
                tiesResolvingScheme = orderingScheme;
            }
            return subPlan.withNewRoot(
                    new LimitNode(
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            analysis.getLimit(limit.get()).getAsLong(),
                            tiesResolvingScheme,
                            false));
        }
        return subPlan;
    }

    private static class GroupingSetsPlan
    {
        private final PlanBuilder subPlan;
        private final List<Set<FieldId>> columnOnlyGroupingSets;
        private final List<List<Symbol>> groupingSets;
        private final Optional<Symbol> groupIdSymbol;

        public GroupingSetsPlan(PlanBuilder subPlan, List<Set<FieldId>> columnOnlyGroupingSets, List<List<Symbol>> groupingSets, Optional<Symbol> groupIdSymbol)
        {
            this.columnOnlyGroupingSets = columnOnlyGroupingSets;
            this.groupingSets = groupingSets;
            this.groupIdSymbol = groupIdSymbol;
            this.subPlan = subPlan;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public List<Set<FieldId>> getColumnOnlyGroupingSets()
        {
            return columnOnlyGroupingSets;
        }

        public List<List<Symbol>> getGroupingSets()
        {
            return groupingSets;
        }

        public Optional<Symbol> getGroupIdSymbol()
        {
            return groupIdSymbol;
        }
    }

    public static class PlanAndMappings
    {
        private final PlanBuilder subPlan;
        private final Map<NodeRef<Expression>, Symbol> mappings;

        public PlanAndMappings(PlanBuilder subPlan, Map<NodeRef<Expression>, Symbol> mappings)
        {
            this.subPlan = subPlan;
            this.mappings = mappings;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Symbol get(Expression expression)
        {
            return tryGet(expression)
                    .orElseThrow(() -> new IllegalArgumentException(format("No mapping for expression: %s (%s)", expression, System.identityHashCode(expression))));
        }

        public Optional<Symbol> tryGet(Expression expression)
        {
            Symbol result = mappings.get(NodeRef.of(expression));

            if (result != null) {
                return Optional.of(result);
            }

            return Optional.empty();
        }
    }

    private static class AggregationAssignment
    {
        private final Symbol symbol;
        private final Expression astExpression;
        private final Aggregation aggregation;

        public AggregationAssignment(Symbol symbol, Expression astExpression, Aggregation aggregation)
        {
            this.astExpression = astExpression;
            this.symbol = symbol;
            this.aggregation = aggregation;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public Expression getAstExpression()
        {
            return astExpression;
        }

        public Aggregation getRewritten()
        {
            return aggregation;
        }
    }

    private static class FrameBoundPlanAndSymbols
    {
        private final PlanBuilder subPlan;
        private final Optional<Symbol> frameBoundSymbol;
        private final Optional<Symbol> sortKeyCoercedForFrameBoundComparison;

        public FrameBoundPlanAndSymbols(PlanBuilder subPlan, Optional<Symbol> frameBoundSymbol, Optional<Symbol> sortKeyCoercedForFrameBoundComparison)
        {
            this.subPlan = subPlan;
            this.frameBoundSymbol = frameBoundSymbol;
            this.sortKeyCoercedForFrameBoundComparison = sortKeyCoercedForFrameBoundComparison;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Optional<Symbol> getFrameBoundSymbol()
        {
            return frameBoundSymbol;
        }

        public Optional<Symbol> getSortKeyCoercedForFrameBoundComparison()
        {
            return sortKeyCoercedForFrameBoundComparison;
        }
    }
}
