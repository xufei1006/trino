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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.CachingCostProvider;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.NewTableLayout;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.RelationId;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.planner.StatisticsAggregationPlanner.TableStatisticAggregation;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DeleteNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.sql.planner.sanity.PlanSanityChecker;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.zip;
import static io.trino.SystemSessionProperties.isCollectPlanStatisticsForAllQueries;
import static io.trino.SystemSessionProperties.isUsePreferredWritePartitioning;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.TableWriterNode.CreateReference;
import static io.trino.sql.planner.plan.TableWriterNode.InsertReference;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import static io.trino.sql.planner.sanity.PlanSanityChecker.DISTRIBUTED_PLAN_SANITY_CHECKER;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LogicalPlanner
{
    public enum Stage
    {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final PlanNodeIdAllocator idAllocator;

    private final Session session;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanSanityChecker planSanityChecker;
    private final SymbolAllocator symbolAllocator = new SymbolAllocator();
    private final Metadata metadata;
    private final TypeOperators typeOperators;
    private final TypeCoercion typeCoercion;
    private final TypeAnalyzer typeAnalyzer;
    private final StatisticsAggregationPlanner statisticsAggregationPlanner;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final WarningCollector warningCollector;

    public LogicalPlanner(
            Session session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        this(session, planOptimizers, DISTRIBUTED_PLAN_SANITY_CHECKER, idAllocator, metadata, typeOperators, typeAnalyzer, statsCalculator, costCalculator, warningCollector);
    }

    public LogicalPlanner(
            Session session,
            List<PlanOptimizer> planOptimizers,
            PlanSanityChecker planSanityChecker,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        this.session = requireNonNull(session, "session is null");
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.planSanityChecker = requireNonNull(planSanityChecker, "planSanityChecker is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeCoercion = new TypeCoercion(metadata::getType);
        this.typeOperators = typeOperators;
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.statisticsAggregationPlanner = new StatisticsAggregationPlanner(symbolAllocator, metadata);
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public Plan plan(Analysis analysis)
    {
        return plan(analysis, OPTIMIZED_AND_VALIDATED);
    }

    public Plan plan(Analysis analysis, Stage stage)
    {
        return plan(analysis, stage, analysis.getStatement() instanceof Explain || isCollectPlanStatisticsForAllQueries(session));
    }

    public Plan plan(Analysis analysis, Stage stage, boolean collectPlanStatistics)
    {
        PlanNode root = planStatement(analysis, analysis.getStatement());

        planSanityChecker.validateIntermediatePlan(root, session, metadata, typeOperators, typeAnalyzer, symbolAllocator.getTypes(), warningCollector);

        if (stage.ordinal() >= OPTIMIZED.ordinal()) {
            String initPlan = PlanPrinter.graphvizLogicalPlan(root, symbolAllocator.getTypes());
            PlanPrinter.printGraphviz(initPlan, "/tmp/plan/3000-init.dot");

            int i = 3001;

            for (PlanOptimizer optimizer : planOptimizers) {
                root = optimizer.optimize(root, session, symbolAllocator.getTypes(), symbolAllocator, idAllocator, warningCollector);
                String graph = PlanPrinter.graphvizLogicalPlan(root, symbolAllocator.getTypes());
                BufferedWriter writer = null;
                try {
                    if (optimizer instanceof IterativeOptimizer) {
                        writer = new BufferedWriter(new FileWriter("/tmp/plan/" + i + "-" + optimizer.getClass().getSimpleName() + ":" + ((IterativeOptimizer) optimizer).getRules() + ".dot"));
                    }
                    else {
                        writer = new BufferedWriter(new FileWriter("/tmp/plan/" + i + "-" + optimizer.getClass().getSimpleName() + ".dot"));
                    }
                    writer.write(graph);
                    writer.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                requireNonNull(root, format("%s returned a null plan", optimizer.getClass().getName()));
                i++;
            }
        }

        if (stage.ordinal() >= OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            planSanityChecker.validateFinalPlan(root, session, metadata, typeOperators, typeAnalyzer, symbolAllocator.getTypes(), warningCollector);
        }

        TypeProvider types = symbolAllocator.getTypes();

        StatsAndCosts statsAndCosts = StatsAndCosts.empty();
        if (collectPlanStatistics) {
            StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
            CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session, types);
            statsAndCosts = StatsAndCosts.create(root, statsProvider, costProvider);
        }
        return new Plan(root, types, statsAndCosts);
    }

    public PlanNode planStatement(Analysis analysis, Statement statement)
    {
        if ((statement instanceof CreateTableAsSelect && analysis.getCreate().get().isCreateTableAsSelectNoOp()) ||
                statement instanceof RefreshMaterializedView && analysis.isSkipMaterializedViewRefresh()) {
            Symbol symbol = symbolAllocator.newSymbol("rows", BIGINT);
            PlanNode source = new ValuesNode(idAllocator.getNextId(), ImmutableList.of(symbol), ImmutableList.of(new Row(ImmutableList.of(new GenericLiteral("BIGINT", "0")))));
            return new OutputNode(idAllocator.getNextId(), source, ImmutableList.of("rows"), ImmutableList.of(symbol));
        }
        return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
    }

    private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement)
    {
        if (statement instanceof CreateTableAsSelect) {
            if (analysis.getCreate().get().isCreateTableAsSelectNoOp()) {
                throw new TrinoException(NOT_SUPPORTED, "CREATE TABLE IF NOT EXISTS is not supported in this context " + statement.getClass().getSimpleName());
            }
            return createTableCreationPlan(analysis, ((CreateTableAsSelect) statement).getQuery());
        }
        if (statement instanceof Analyze) {
            return createAnalyzePlan(analysis, (Analyze) statement);
        }
        if (statement instanceof Insert) {
            checkState(analysis.getInsert().isPresent(), "Insert handle is missing");
            return createInsertPlan(analysis, (Insert) statement);
        }
        else if (statement instanceof RefreshMaterializedView) {
            checkState(analysis.getRefreshMaterializedView().isPresent(), "RefreshMaterializedViewAnalysis handle is missing");
            return createRefreshMaterializedViewPlan(analysis);
        }
        else if (statement instanceof Delete) {
            return createDeletePlan(analysis, (Delete) statement);
        }
        if (statement instanceof Query) {
            return createRelationPlan(analysis, (Query) statement);
        }
        if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
            return createExplainAnalyzePlan(analysis, (Explain) statement);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported statement type " + statement.getClass().getSimpleName());
    }

    private RelationPlan createExplainAnalyzePlan(Analysis analysis, Explain statement)
    {
        RelationPlan underlyingPlan = planStatementWithoutOutput(analysis, statement.getStatement());
        PlanNode root = underlyingPlan.getRoot();
        Scope scope = analysis.getScope(statement);
        Symbol outputSymbol = symbolAllocator.newSymbol(scope.getRelationType().getFieldByIndex(0));

        ImmutableList.Builder<Symbol> actualOutputs = ImmutableList.builder();
        RelationType outputDescriptor = analysis.getOutputDescriptor(statement.getStatement());
        for (Field field : outputDescriptor.getVisibleFields()) {
            int fieldIndex = outputDescriptor.indexOf(field);
            Symbol symbol = underlyingPlan.getSymbol(fieldIndex);
            actualOutputs.add(symbol);
        }
        root = new ExplainAnalyzeNode(idAllocator.getNextId(), root, outputSymbol, actualOutputs.build(), statement.isVerbose());
        return new RelationPlan(root, scope, ImmutableList.of(outputSymbol), Optional.empty());
    }

    private RelationPlan createAnalyzePlan(Analysis analysis, Analyze analyzeStatement)
    {
        TableHandle targetTable = analysis.getAnalyzeTarget().get();

        // Plan table scan
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTable);
        ImmutableList.Builder<Symbol> tableScanOutputs = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> symbolToColumnHandle = ImmutableMap.builder();
        ImmutableMap.Builder<String, Symbol> columnNameToSymbol = ImmutableMap.builder();
        TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTable);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Symbol symbol = symbolAllocator.newSymbol(column.getName(), column.getType());
            tableScanOutputs.add(symbol);
            symbolToColumnHandle.put(symbol, columnHandles.get(column.getName()));
            columnNameToSymbol.put(column.getName(), symbol);
        }

        TableStatisticsMetadata tableStatisticsMetadata = metadata.getStatisticsCollectionMetadata(
                session,
                targetTable.getCatalogName().getCatalogName(),
                tableMetadata.getMetadata());

        TableStatisticAggregation tableStatisticAggregation = statisticsAggregationPlanner.createStatisticsAggregation(tableStatisticsMetadata, columnNameToSymbol.build());
        StatisticAggregations statisticAggregations = tableStatisticAggregation.getAggregations();
        List<Symbol> groupingSymbols = statisticAggregations.getGroupingSymbols();

        PlanNode planNode = new StatisticsWriterNode(
                idAllocator.getNextId(),
                new AggregationNode(
                        idAllocator.getNextId(),
                        TableScanNode.newInstance(idAllocator.getNextId(), targetTable, tableScanOutputs.build(), symbolToColumnHandle.build(), false),
                        statisticAggregations.getAggregations(),
                        singleGroupingSet(groupingSymbols),
                        ImmutableList.of(),
                        AggregationNode.Step.SINGLE,
                        Optional.empty(),
                        Optional.empty()),
                new StatisticsWriterNode.WriteStatisticsReference(targetTable),
                symbolAllocator.newSymbol("rows", BIGINT),
                tableStatisticsMetadata.getTableStatistics().contains(ROW_COUNT),
                tableStatisticAggregation.getDescriptor());
        return new RelationPlan(planNode, analysis.getScope(analyzeStatement), planNode.getOutputSymbols(), Optional.empty());
    }

    private RelationPlan createTableCreationPlan(Analysis analysis, Query query)
    {
        Analysis.Create create = analysis.getCreate().get();
        QualifiedObjectName destination = create.getDestination().get();

        RelationPlan plan = createRelationPlan(analysis, query);
        if (!create.isCreateTableAsSelectWithData()) {
            PlanNode root = new LimitNode(idAllocator.getNextId(), plan.getRoot(), 0L, false);
            plan = new RelationPlan(root, plan.getScope(), plan.getFieldMappings(), Optional.empty());
        }

        ConnectorTableMetadata tableMetadata = create.getMetadata().get();

        Optional<NewTableLayout> newTableLayout = create.getLayout();

        List<String> columnNames = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden()) // todo this filter is redundant
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, destination.getCatalogName(), tableMetadata);

        return createTableWriterPlan(
                analysis,
                plan,
                new CreateReference(destination.getCatalogName(), tableMetadata, newTableLayout),
                columnNames,
                tableMetadata.getColumns(),
                newTableLayout,
                statisticsMetadata);
    }

    private RelationPlan getInsertPlan(
            Analysis analysis,
            Query query,
            TableHandle tableHandle,
            List<ColumnHandle> insertColumns,
            Optional<NewTableLayout> newTableLayout,
            boolean isMaterializedViewRefresh,
            WriterTarget writerTarget)
    {
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);

        RelationPlan plan = createRelationPlan(analysis, query);

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, tableHandle);
        Assignments.Builder assignments = Assignments.builder();
        boolean supportsMissingColumnsOnInsert = metadata.supportsMissingColumnsOnInsert(session, tableHandle);
        ImmutableList.Builder<ColumnMetadata> insertedColumnsBuilder = ImmutableList.builder();

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.isHidden()) {
                continue;
            }
            Symbol output = symbolAllocator.newSymbol(column.getName(), column.getType());
            int index = insertColumns.indexOf(columns.get(column.getName()));
            if (index < 0) {
                if (supportsMissingColumnsOnInsert) {
                    continue;
                }
                Expression cast = new Cast(new NullLiteral(), toSqlType(column.getType()));
                assignments.put(output, cast);
                insertedColumnsBuilder.add(column);
            }
            else {
                Symbol input = plan.getSymbol(index);
                Type tableType = column.getType();
                Type queryType = symbolAllocator.getTypes().get(input);

                if (queryType.equals(tableType) || typeCoercion.isTypeOnlyCoercion(queryType, tableType)) {
                    assignments.put(output, input.toSymbolReference());
                }
                else {
                    Expression cast = noTruncationCast(input.toSymbolReference(), queryType, tableType);
                    assignments.put(output, cast);
                }
                insertedColumnsBuilder.add(column);
            }
        }

        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());

        List<ColumnMetadata> insertedColumns = insertedColumnsBuilder.build();
        List<Field> fields = insertedColumns.stream()
                .map(column -> Field.newUnqualified(column.getName(), column.getType()))
                .collect(toImmutableList());
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();

        plan = new RelationPlan(projectNode, scope, projectNode.getOutputSymbols(), Optional.empty());

        List<String> insertedTableColumnNames = insertedColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        String catalogName = tableHandle.getCatalogName().getCatalogName();
        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, catalogName, tableMetadata.getMetadata());

        if (isMaterializedViewRefresh) {
            return createTableWriterPlan(
                    analysis,
                    plan,
                    requireNonNull(writerTarget, "writerTarget for materialized view refresh is null"),
                    insertedTableColumnNames,
                    insertedColumns,
                    newTableLayout,
                    statisticsMetadata);
        }
        InsertReference insertTarget = new InsertReference(
                tableHandle,
                insertedTableColumnNames.stream()
                        .map(columns::get)
                        .collect(toImmutableList()));
        return createTableWriterPlan(
                analysis,
                plan,
                insertTarget,
                insertedTableColumnNames,
                insertedColumns,
                newTableLayout,
                statisticsMetadata);
    }

    private RelationPlan createInsertPlan(Analysis analysis, Insert insertStatement)
    {
        Analysis.Insert insert = analysis.getInsert().get();
        TableHandle tableHandle = insert.getTarget();
        Query query = insertStatement.getQuery();
        Optional<NewTableLayout> newTableLayout = insert.getNewTableLayout();
        return getInsertPlan(analysis, query, tableHandle, insert.getColumns(), newTableLayout, false, null);
    }

    private RelationPlan createRefreshMaterializedViewPlan(Analysis analysis)
    {
        Analysis.RefreshMaterializedViewAnalysis viewAnalysis = analysis.getRefreshMaterializedView().get();
        TableHandle tableHandle = viewAnalysis.getTarget();
        Query query = viewAnalysis.getQuery();
        Optional<NewTableLayout> newTableLayout = metadata.getInsertLayout(session, viewAnalysis.getTarget());
        TableWriterNode.RefreshMaterializedViewReference writerTarget = new TableWriterNode.RefreshMaterializedViewReference(viewAnalysis.getMaterializedViewName(),
                tableHandle, new ArrayList<>(analysis.getTables()));
        return getInsertPlan(analysis, query, tableHandle, viewAnalysis.getColumns(), newTableLayout, true, writerTarget);
    }

    private RelationPlan createTableWriterPlan(
            Analysis analysis,
            RelationPlan plan,
            WriterTarget target,
            List<String> columnNames,
            List<ColumnMetadata> columnMetadataList,
            Optional<NewTableLayout> writeTableLayout,
            TableStatisticsMetadata statisticsMetadata)
    {
        PlanNode source = plan.getRoot();

        List<Symbol> symbols = plan.getFieldMappings();

        Optional<PartitioningScheme> partitioningScheme = Optional.empty();
        if (writeTableLayout.isPresent()) {
            List<Symbol> partitionFunctionArguments = new ArrayList<>();
            writeTableLayout.get().getPartitionColumns().stream()
                    .mapToInt(columnNames::indexOf)
                    .mapToObj(symbols::get)
                    .forEach(partitionFunctionArguments::add);

            List<Symbol> outputLayout = new ArrayList<>(symbols);

            Optional<PartitioningHandle> partitioningHandle = writeTableLayout.get().getPartitioning();
            if (partitioningHandle.isPresent()) {
                partitioningScheme = Optional.of(new PartitioningScheme(
                        Partitioning.create(partitioningHandle.get(), partitionFunctionArguments),
                        outputLayout));
            }
            else if (isUsePreferredWritePartitioning(session)) {
                // TODO: move to iterative optimizer and use CBO
                // empty connector partitioning handle means evenly partitioning on partitioning columns
                partitioningScheme = Optional.of(new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, partitionFunctionArguments),
                        outputLayout));
            }
        }

        verify(columnNames.size() == symbols.size(), "columnNames.size() != symbols.size(): %s and %s", columnNames, symbols);
        Map<String, Symbol> columnToSymbolMap = zip(columnNames.stream(), symbols.stream(), SimpleImmutableEntry::new)
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        Set<Symbol> notNullColumnSymbols = columnMetadataList.stream()
                .filter(column -> !column.isNullable())
                .map(ColumnMetadata::getName)
                .map(columnToSymbolMap::get)
                .collect(toImmutableSet());

        if (!statisticsMetadata.isEmpty()) {
            TableStatisticAggregation result = statisticsAggregationPlanner.createStatisticsAggregation(statisticsMetadata, columnToSymbolMap);

            StatisticAggregations.Parts aggregations = result.getAggregations().createPartialAggregations(symbolAllocator, metadata);

            // partial aggregation is run within the TableWriteOperator to calculate the statistics for
            // the data consumed by the TableWriteOperator
            // final aggregation is run within the TableFinishOperator to summarize collected statistics
            // by the partial aggregation from all of the writer nodes
            StatisticAggregations partialAggregation = aggregations.getPartialAggregation();

            TableFinishNode commitNode = new TableFinishNode(
                    idAllocator.getNextId(),
                    new TableWriterNode(
                            idAllocator.getNextId(),
                            source,
                            target,
                            symbolAllocator.newSymbol("partialrows", BIGINT),
                            symbolAllocator.newSymbol("fragment", VARBINARY),
                            symbols,
                            columnNames,
                            notNullColumnSymbols,
                            partitioningScheme,
                            Optional.of(partialAggregation),
                            Optional.of(result.getDescriptor().map(aggregations.getMappings()::get))),
                    target,
                    symbolAllocator.newSymbol("rows", BIGINT),
                    Optional.of(aggregations.getFinalAggregation()),
                    Optional.of(result.getDescriptor()));

            return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputSymbols(), Optional.empty());
        }

        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                new TableWriterNode(
                        idAllocator.getNextId(),
                        source,
                        target,
                        symbolAllocator.newSymbol("partialrows", BIGINT),
                        symbolAllocator.newSymbol("fragment", VARBINARY),
                        symbols,
                        columnNames,
                        notNullColumnSymbols,
                        partitioningScheme,
                        Optional.empty(),
                        Optional.empty()),
                target,
                symbolAllocator.newSymbol("rows", BIGINT),
                Optional.empty(),
                Optional.empty());

        return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputSymbols(), Optional.empty());
    }

    /*
    According to the standard, for the purpose of store assignment (INSERT),
    no non-space characters of a character string, and no non-zero octets
    of a binary string must be lost when the inserted value is truncated to
    fit in the target column type.
    The following method returns a cast from source type to target type
    with a guarantee of no illegal truncation.
    TODO Once BINARY and parametric VARBINARY types are supported, they should be handled here.
    TODO This workaround is insufficient to handle structural types
     */
    private Expression noTruncationCast(Expression expression, Type fromType, Type toType)
    {
        if (fromType instanceof UnknownType || (!(toType instanceof VarcharType) && !(toType instanceof CharType))) {
            return new Cast(expression, toSqlType(toType));
        }
        int targetLength;
        if (toType instanceof VarcharType) {
            if (((VarcharType) toType).isUnbounded()) {
                return new Cast(expression, toSqlType(toType));
            }
            targetLength = ((VarcharType) toType).getBoundedLength();
        }
        else {
            targetLength = ((CharType) toType).getLength();
        }

        checkState(fromType instanceof VarcharType || fromType instanceof CharType, "inserting non-character value to column of character type");
        ResolvedFunction spaceTrimmedLength = metadata.resolveFunction(QualifiedName.of("$space_trimmed_length"), fromTypes(VARCHAR));
        ResolvedFunction fail = metadata.resolveFunction(QualifiedName.of("fail"), fromTypes(VARCHAR));

        return new IfExpression(
                // check if the trimmed value fits in the target type
                new ComparisonExpression(
                        GREATER_THAN_OR_EQUAL,
                        new GenericLiteral("BIGINT", Integer.toString(targetLength)),
                        new CoalesceExpression(
                                new FunctionCall(
                                        spaceTrimmedLength.toQualifiedName(),
                                        ImmutableList.of(new Cast(expression, toSqlType(VARCHAR)))),
                                new GenericLiteral("BIGINT", "0"))),
                new Cast(expression, toSqlType(toType)),
                new Cast(
                        new FunctionCall(
                                fail.toQualifiedName(),
                                ImmutableList.of(new Cast(
                                        new StringLiteral(format(
                                                "Cannot truncate non-space characters when casting from %s to %s on INSERT",
                                                fromType.getDisplayName(),
                                                toType.getDisplayName())),
                                        toSqlType(VARCHAR)))),
                        toSqlType(toType)));
    }

    private RelationPlan createDeletePlan(Analysis analysis, Delete node)
    {
        DeleteNode deleteNode = new QueryPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), metadata, Optional.empty(), session, ImmutableMap.of())
                .plan(node);

        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                deleteNode,
                deleteNode.getTarget(),
                symbolAllocator.newSymbol("rows", BIGINT),
                Optional.empty(),
                Optional.empty());

        return new RelationPlan(commitNode, analysis.getScope(node), commitNode.getOutputSymbols(), Optional.empty());
    }

    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        int columnNumber = 0;
        RelationType outputDescriptor = analysis.getOutputDescriptor();
        for (Field field : outputDescriptor.getVisibleFields()) {
            String name = field.getName().orElse("_col" + columnNumber);
            names.add(name);

            int fieldIndex = outputDescriptor.indexOf(field);
            Symbol symbol = plan.getSymbol(fieldIndex);
            outputs.add(symbol);

            columnNumber++;
        }

        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), names.build(), outputs.build());
    }

    public static AtomicInteger id = new AtomicInteger(2000);

    private RelationPlan createRelationPlan(Analysis analysis, Query query)
    {
        id.set(2000);
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), metadata, Optional.empty(), session, ImmutableMap.of())
                .process(query, null);
    }

    private static Map<NodeRef<LambdaArgumentDeclaration>, Symbol> buildLambdaDeclarationToSymbolMap(Analysis analysis, SymbolAllocator symbolAllocator)
    {
        Map<Key, Symbol> allocations = new HashMap<>();
        Map<NodeRef<LambdaArgumentDeclaration>, Symbol> result = new LinkedHashMap<>();

        for (Entry<NodeRef<Expression>, Type> entry : analysis.getTypes().entrySet()) {
            if (!(entry.getKey().getNode() instanceof LambdaArgumentDeclaration)) {
                continue;
            }

            LambdaArgumentDeclaration argument = (LambdaArgumentDeclaration) entry.getKey().getNode();
            Key key = new Key(argument, entry.getValue());

            // Allocate the same symbol for all lambda argument names with a given type. This is needed to be able to
            // properly identify multiple instances of syntactically equal lambda expressions during planning as expressions
            // get rewritten via TranslationMap
            Symbol symbol = allocations.get(key);
            if (symbol == null) {
                symbol = symbolAllocator.newSymbol(argument, entry.getValue());
                allocations.put(key, symbol);
            }

            result.put(NodeRef.of(argument), symbol);
        }

        return result;
    }

    private static class Key
    {
        private final LambdaArgumentDeclaration argument;
        private final Type type;

        public Key(LambdaArgumentDeclaration argument, Type type)
        {
            this.argument = requireNonNull(argument, "argument is null");
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(argument, key.argument) &&
                    Objects.equals(type, key.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(argument, type);
        }
    }
}
