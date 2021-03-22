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
import com.google.common.collect.ImmutableSet;
import io.trino.SystemSessionProperties;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculator.EstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.StatsCalculator;
import io.trino.cost.TaskCountEstimator;
import io.trino.execution.TaskManagerConfig;
import io.trino.metadata.Metadata;
import io.trino.spi.type.TypeOperators;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.AddExchangesBelowPartialAggregationOverGroupIdRuleSet;
import io.trino.sql.planner.iterative.rule.AddIntermediateAggregations;
import io.trino.sql.planner.iterative.rule.ApplyTableScanRedirection;
import io.trino.sql.planner.iterative.rule.CanonicalizeExpressions;
import io.trino.sql.planner.iterative.rule.CreatePartialTopN;
import io.trino.sql.planner.iterative.rule.DesugarArrayConstructor;
import io.trino.sql.planner.iterative.rule.DesugarAtTimeZone;
import io.trino.sql.planner.iterative.rule.DesugarCurrentPath;
import io.trino.sql.planner.iterative.rule.DesugarCurrentUser;
import io.trino.sql.planner.iterative.rule.DesugarLambdaExpression;
import io.trino.sql.planner.iterative.rule.DesugarLike;
import io.trino.sql.planner.iterative.rule.DesugarRowSubscript;
import io.trino.sql.planner.iterative.rule.DesugarTryExpression;
import io.trino.sql.planner.iterative.rule.DetermineJoinDistributionType;
import io.trino.sql.planner.iterative.rule.DetermineSemiJoinDistributionType;
import io.trino.sql.planner.iterative.rule.EliminateCrossJoins;
import io.trino.sql.planner.iterative.rule.EvaluateZeroSample;
import io.trino.sql.planner.iterative.rule.ExtractDereferencesFromFilterAboveScan;
import io.trino.sql.planner.iterative.rule.ExtractSpatialJoins;
import io.trino.sql.planner.iterative.rule.GatherAndMergeWindows;
import io.trino.sql.planner.iterative.rule.ImplementBernoulliSampleAsFilter;
import io.trino.sql.planner.iterative.rule.ImplementExceptAll;
import io.trino.sql.planner.iterative.rule.ImplementExceptDistinctAsUnion;
import io.trino.sql.planner.iterative.rule.ImplementFilteredAggregations;
import io.trino.sql.planner.iterative.rule.ImplementIntersectAll;
import io.trino.sql.planner.iterative.rule.ImplementIntersectDistinctAsUnion;
import io.trino.sql.planner.iterative.rule.ImplementLimitWithTies;
import io.trino.sql.planner.iterative.rule.ImplementOffset;
import io.trino.sql.planner.iterative.rule.InlineProjections;
import io.trino.sql.planner.iterative.rule.MergeExcept;
import io.trino.sql.planner.iterative.rule.MergeFilters;
import io.trino.sql.planner.iterative.rule.MergeIntersect;
import io.trino.sql.planner.iterative.rule.MergeLimitOverProjectWithSort;
import io.trino.sql.planner.iterative.rule.MergeLimitWithDistinct;
import io.trino.sql.planner.iterative.rule.MergeLimitWithSort;
import io.trino.sql.planner.iterative.rule.MergeLimitWithTopN;
import io.trino.sql.planner.iterative.rule.MergeLimits;
import io.trino.sql.planner.iterative.rule.MergeUnion;
import io.trino.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import io.trino.sql.planner.iterative.rule.OptimizeDuplicateInsensitiveJoins;
import io.trino.sql.planner.iterative.rule.PruneAggregationColumns;
import io.trino.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneApplyColumns;
import io.trino.sql.planner.iterative.rule.PruneApplyCorrelation;
import io.trino.sql.planner.iterative.rule.PruneApplySourceColumns;
import io.trino.sql.planner.iterative.rule.PruneAssignUniqueIdColumns;
import io.trino.sql.planner.iterative.rule.PruneCorrelatedJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneCorrelatedJoinCorrelation;
import io.trino.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import io.trino.sql.planner.iterative.rule.PruneDeleteSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneDistinctAggregation;
import io.trino.sql.planner.iterative.rule.PruneDistinctLimitSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneEnforceSingleRowColumns;
import io.trino.sql.planner.iterative.rule.PruneExceptSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneExchangeColumns;
import io.trino.sql.planner.iterative.rule.PruneExchangeSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneExplainAnalyzeSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneFilterColumns;
import io.trino.sql.planner.iterative.rule.PruneGroupIdColumns;
import io.trino.sql.planner.iterative.rule.PruneGroupIdSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneIndexJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneIndexSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneIntersectSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneJoinChildrenColumns;
import io.trino.sql.planner.iterative.rule.PruneJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneLimitColumns;
import io.trino.sql.planner.iterative.rule.PruneMarkDistinctColumns;
import io.trino.sql.planner.iterative.rule.PruneOffsetColumns;
import io.trino.sql.planner.iterative.rule.PruneOrderByInAggregation;
import io.trino.sql.planner.iterative.rule.PruneOutputSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneProjectColumns;
import io.trino.sql.planner.iterative.rule.PruneRowNumberColumns;
import io.trino.sql.planner.iterative.rule.PruneSampleColumns;
import io.trino.sql.planner.iterative.rule.PruneSemiJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneSortColumns;
import io.trino.sql.planner.iterative.rule.PruneSpatialJoinChildrenColumns;
import io.trino.sql.planner.iterative.rule.PruneSpatialJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PruneTableWriterSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneTopNColumns;
import io.trino.sql.planner.iterative.rule.PruneTopNRankingColumns;
import io.trino.sql.planner.iterative.rule.PruneUnionColumns;
import io.trino.sql.planner.iterative.rule.PruneUnionSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneUnnestColumns;
import io.trino.sql.planner.iterative.rule.PruneUnnestSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneValuesColumns;
import io.trino.sql.planner.iterative.rule.PruneWindowColumns;
import io.trino.sql.planner.iterative.rule.PushAggregationIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import io.trino.sql.planner.iterative.rule.PushDeleteIntoConnector;
import io.trino.sql.planner.iterative.rule.PushDistinctLimitIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughFilter;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughJoin;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughProject;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughSemiJoin;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughUnnest;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughAssignUniqueId;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughLimit;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughRowNumber;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughSort;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughTopN;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughTopNRanking;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughWindow;
import io.trino.sql.planner.iterative.rule.PushLimitIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import io.trino.sql.planner.iterative.rule.PushLimitThroughOffset;
import io.trino.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import io.trino.sql.planner.iterative.rule.PushLimitThroughProject;
import io.trino.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import io.trino.sql.planner.iterative.rule.PushLimitThroughUnion;
import io.trino.sql.planner.iterative.rule.PushOffsetThroughProject;
import io.trino.sql.planner.iterative.rule.PushPartialAggregationThroughExchange;
import io.trino.sql.planner.iterative.rule.PushPartialAggregationThroughJoin;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushPredicateThroughProjectIntoRowNumber;
import io.trino.sql.planner.iterative.rule.PushPredicateThroughProjectIntoWindow;
import io.trino.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushProjectionThroughExchange;
import io.trino.sql.planner.iterative.rule.PushProjectionThroughUnion;
import io.trino.sql.planner.iterative.rule.PushRemoteExchangeThroughAssignUniqueId;
import io.trino.sql.planner.iterative.rule.PushSampleIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushTableWriteThroughUnion;
import io.trino.sql.planner.iterative.rule.PushTopNIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushTopNThroughOuterJoin;
import io.trino.sql.planner.iterative.rule.PushTopNThroughProject;
import io.trino.sql.planner.iterative.rule.PushTopNThroughUnion;
import io.trino.sql.planner.iterative.rule.PushdownFilterIntoRowNumber;
import io.trino.sql.planner.iterative.rule.PushdownFilterIntoWindow;
import io.trino.sql.planner.iterative.rule.PushdownLimitIntoRowNumber;
import io.trino.sql.planner.iterative.rule.PushdownLimitIntoWindow;
import io.trino.sql.planner.iterative.rule.RemoveAggregationInSemiJoin;
import io.trino.sql.planner.iterative.rule.RemoveDuplicateConditions;
import io.trino.sql.planner.iterative.rule.RemoveEmptyDelete;
import io.trino.sql.planner.iterative.rule.RemoveFullSample;
import io.trino.sql.planner.iterative.rule.RemoveRedundantCrossJoin;
import io.trino.sql.planner.iterative.rule.RemoveRedundantDistinctLimit;
import io.trino.sql.planner.iterative.rule.RemoveRedundantEnforceSingleRowNode;
import io.trino.sql.planner.iterative.rule.RemoveRedundantExists;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.iterative.rule.RemoveRedundantJoin;
import io.trino.sql.planner.iterative.rule.RemoveRedundantLimit;
import io.trino.sql.planner.iterative.rule.RemoveRedundantOffset;
import io.trino.sql.planner.iterative.rule.RemoveRedundantSort;
import io.trino.sql.planner.iterative.rule.RemoveRedundantTableScanPredicate;
import io.trino.sql.planner.iterative.rule.RemoveRedundantTopN;
import io.trino.sql.planner.iterative.rule.RemoveTrivialFilters;
import io.trino.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import io.trino.sql.planner.iterative.rule.RemoveUnreferencedScalarSubqueries;
import io.trino.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import io.trino.sql.planner.iterative.rule.ReorderJoins;
import io.trino.sql.planner.iterative.rule.ReplaceWindowWithRowNumber;
import io.trino.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import io.trino.sql.planner.iterative.rule.SimplifyCountOverConstant;
import io.trino.sql.planner.iterative.rule.SimplifyExpressions;
import io.trino.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedDistinctAggregationWithProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedDistinctAggregationWithoutProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedGlobalAggregationWithProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedGlobalAggregationWithoutProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedGroupedAggregationWithProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedGroupedAggregationWithoutProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedJoinToJoin;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedSingleRowSubqueryToProject;
import io.trino.sql.planner.iterative.rule.TransformExistsApplyToCorrelatedJoin;
import io.trino.sql.planner.iterative.rule.TransformFilteringSemiJoinToInnerJoin;
import io.trino.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import io.trino.sql.planner.iterative.rule.TransformUncorrelatedSubqueryToJoin;
import io.trino.sql.planner.iterative.rule.UnwrapCastInComparison;
import io.trino.sql.planner.optimizations.AddExchanges;
import io.trino.sql.planner.optimizations.AddLocalExchanges;
import io.trino.sql.planner.optimizations.BeginTableWrite;
import io.trino.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import io.trino.sql.planner.optimizations.HashGenerationOptimizer;
import io.trino.sql.planner.optimizations.IndexJoinOptimizer;
import io.trino.sql.planner.optimizations.LimitPushDown;
import io.trino.sql.planner.optimizations.MetadataQueryOptimizer;
import io.trino.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.optimizations.PredicatePushDown;
import io.trino.sql.planner.optimizations.PruneUnreferencedOutputs;
import io.trino.sql.planner.optimizations.ReplicateSemiJoinInDelete;
import io.trino.sql.planner.optimizations.TableDeleteOptimizer;
import io.trino.sql.planner.optimizations.TransformQuantifiedComparisonApplyToCorrelatedJoin;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.sql.planner.optimizations.WindowFilterPushDown;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static io.trino.SystemSessionProperties.isIterativeRuleBasedColumnPruning;

public class PlanOptimizers
{
    private final List<PlanOptimizer> optimizers;
    private final RuleStatsRecorder ruleStats;
    private final OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();
    private final MBeanExporter exporter;

    @Inject
    public PlanOptimizers(
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            MBeanExporter exporter,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            @EstimatedExchanges CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            RuleStatsRecorder ruleStats)
    {
        this(metadata,
                typeOperators,
                typeAnalyzer,
                taskManagerConfig,
                false,
                exporter,
                splitManager,
                pageSourceManager,
                statsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                costComparator,
                taskCountEstimator,
                ruleStats);
    }

    @PostConstruct
    public void initialize()
    {
        ruleStats.export(exporter);
        optimizerStats.export(exporter);
    }

    @PreDestroy
    public void destroy()
    {
        ruleStats.unexport(exporter);
        optimizerStats.unexport(exporter);
    }

    public PlanOptimizers(
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            boolean forceSingleNode,
            MBeanExporter exporter,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            RuleStatsRecorder ruleStats)
    {
        this.ruleStats = ruleStats;
        this.exporter = exporter;
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                new PruneApplyColumns(),
                new PruneApplyCorrelation(),
                new PruneApplySourceColumns(),
                new PruneAssignUniqueIdColumns(),
                new PruneCorrelatedJoinColumns(),
                new PruneCorrelatedJoinCorrelation(),
                new PruneDeleteSourceColumns(),
                new PruneDistinctLimitSourceColumns(),
                new PruneEnforceSingleRowColumns(),
                new PruneExceptSourceColumns(),
                new PruneExchangeColumns(),
                new PruneExchangeSourceColumns(),
                new PruneExplainAnalyzeSourceColumns(),
                new PruneFilterColumns(),
                new PruneGroupIdColumns(),
                new PruneGroupIdSourceColumns(),
                new PruneIndexJoinColumns(),
                new PruneIndexSourceColumns(),
                new PruneIntersectSourceColumns(),
                new PruneJoinChildrenColumns(),
                new PruneJoinColumns(),
                new PruneLimitColumns(),
                new PruneMarkDistinctColumns(),
                new PruneOffsetColumns(),
                new PruneOutputSourceColumns(),
                new PruneProjectColumns(),
                new PruneRowNumberColumns(),
                new PruneSampleColumns(),
                new PruneSemiJoinColumns(),
                new PruneSemiJoinFilteringSourceColumns(),
                new PruneSortColumns(),
                new PruneSpatialJoinChildrenColumns(),
                new PruneSpatialJoinColumns(),
                new PruneTableScanColumns(metadata),
                new PruneTableWriterSourceColumns(),
                new PruneTopNColumns(),
                new PruneTopNRankingColumns(),
                new PruneUnionColumns(),
                new PruneUnionSourceColumns(),
                new PruneUnnestColumns(),
                new PruneUnnestSourceColumns(),
                new PruneValuesColumns(),
                new PruneWindowColumns());

        Set<Rule<?>> projectionPushdownRules = ImmutableSet.of(
                new PushProjectionIntoTableScan(metadata, typeAnalyzer),
                new PushProjectionThroughUnion(),
                new PushProjectionThroughExchange(),
                // Dereference pushdown rules
                new PushDownDereferenceThroughProject(typeAnalyzer),
                new PushDownDereferenceThroughUnnest(typeAnalyzer),
                new PushDownDereferenceThroughSemiJoin(typeAnalyzer),
                new PushDownDereferenceThroughJoin(typeAnalyzer),
                new PushDownDereferenceThroughFilter(typeAnalyzer),
                new ExtractDereferencesFromFilterAboveScan(typeAnalyzer),
                new PushDownDereferencesThroughLimit(typeAnalyzer),
                new PushDownDereferencesThroughSort(typeAnalyzer),
                new PushDownDereferencesThroughAssignUniqueId(typeAnalyzer),
                new PushDownDereferencesThroughWindow(typeAnalyzer),
                new PushDownDereferencesThroughTopN(typeAnalyzer),
                new PushDownDereferencesThroughRowNumber(typeAnalyzer),
                new PushDownDereferencesThroughTopNRanking(typeAnalyzer));

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new InlineProjections(),
                        new RemoveRedundantIdentityProjections()),
                "inlineProjections");

        IterativeOptimizer projectionPushDown = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                projectionPushdownRules,
                "projectionPushDown");

        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(new SimplifyExpressions(metadata, typeAnalyzer).rules())
                        .addAll(new UnwrapCastInComparison(metadata, typeOperators, typeAnalyzer).rules())
                        .addAll(new RemoveDuplicateConditions(metadata).rules())
                        .addAll(new CanonicalizeExpressions(metadata, typeAnalyzer).rules())
                        .add(new RemoveTrivialFilters())
                        .build(),
                "simplifyOptimizer");

        IterativeOptimizer columnPruningOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                session -> !isIterativeRuleBasedColumnPruning(session),
                ImmutableList.of(new PruneUnreferencedOutputs(metadata)),
                columnPruningRules,
                "columnPruningOptimizer");

        builder.add(
                // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .addAll(new DesugarAtTimeZone(metadata, typeAnalyzer).rules())
                                .addAll(new DesugarCurrentUser(metadata).rules())
                                .addAll(new DesugarCurrentPath(metadata).rules())
                                .addAll(new DesugarTryExpression(metadata, typeAnalyzer).rules())
                                .addAll(new DesugarRowSubscript(typeAnalyzer).rules())
                                .build(),
                        "DesugarExpression"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new CanonicalizeExpressions(metadata, typeAnalyzer).rules(),
                        "CanonicalizeExpressions"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(columnPruningRules)
                                .addAll(projectionPushdownRules)
                                .addAll(ImmutableSet.of(
                                        new MergeFilters(metadata),
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveFullSample(),
                                        new EvaluateZeroSample(),
                                        new PushOffsetThroughProject(),
                                        new PushLimitThroughOffset(),
                                        new PushLimitThroughProject(),
                                        new MergeLimits(),
                                        new MergeLimitWithSort(),
                                        new MergeLimitOverProjectWithSort(),
                                        new MergeLimitWithTopN(),
                                        new PushLimitThroughMarkDistinct(),
                                        new PushLimitThroughOuterJoin(),
                                        new PushLimitThroughSemiJoin(),
                                        new PushLimitThroughUnion(),
                                        new RemoveTrivialFilters(),
                                        new RemoveRedundantLimit(),
                                        new RemoveRedundantOffset(),
                                        new RemoveRedundantSort(),
                                        new RemoveRedundantTopN(),
                                        new RemoveRedundantDistinctLimit(),
                                        new RemoveRedundantCrossJoin(),
                                        new RemoveRedundantJoin(),
                                        new RemoveRedundantEnforceSingleRowNode(),
                                        new RemoveRedundantExists(),
                                        new ImplementFilteredAggregations(metadata),
                                        new SingleDistinctAggregationToGroupBy(),
                                        new MultipleDistinctAggregationToMarkDistinct(),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(metadata),
                                        new PruneOrderByInAggregation(metadata),
                                        new RewriteSpatialPartitioningAggregation(metadata),
                                        new SimplifyCountOverConstant(metadata)))
                                .build(),
                        "MergeXXXPushXXXRemoveXXXPruneXXXRewriteXXXSimplifyXXX"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ImplementOffset(),
                                new ImplementLimitWithTies(metadata)),
                        "LIMIT"),
                simplifyOptimizer,
                new UnaliasSymbolReferences(metadata),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections()),
                        "RemoveRedundantIdentityProjections"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new MergeUnion(),
                                new MergeIntersect(),
                                new MergeExcept(),
                                new PruneDistinctAggregation()),
                        "MergeUnion-MergeIntersect-MergeExcept"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ImplementIntersectDistinctAsUnion(metadata),
                                new ImplementExceptDistinctAsUnion(metadata),
                                new ImplementIntersectAll(metadata),
                                new ImplementExceptAll(metadata)),
                        "ImplementIntersectDistinctAsUnion"),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                columnPruningOptimizer,
                inlineProjections,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        columnPruningRules,
                        "columnPruningRules"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformExistsApplyToCorrelatedJoin(metadata)),
                        "TransformExistsApplyToCorrelatedJoin"),
                new TransformQuantifiedComparisonApplyToCorrelatedJoin(metadata),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantEnforceSingleRowNode(),
                                new RemoveUnreferencedScalarSubqueries(),
                                new TransformUncorrelatedSubqueryToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedJoinToJoin(metadata),
                                new TransformCorrelatedGlobalAggregationWithProjection(metadata),
                                new TransformCorrelatedGlobalAggregationWithoutProjection(metadata),
                                new TransformCorrelatedDistinctAggregationWithProjection(metadata),
                                new TransformCorrelatedDistinctAggregationWithoutProjection(metadata),
                                new TransformCorrelatedGroupedAggregationWithProjection(metadata),
                                new TransformCorrelatedGroupedAggregationWithoutProjection(metadata)),
                        "TransformUncorrelatedXXXTransformCorrelatedXXX"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(metadata), // must be run after columnPruningOptimizer
                                new TransformCorrelatedScalarSubquery(metadata), // must be run after TransformCorrelatedAggregation rules
                                new TransformCorrelatedJoinToJoin(metadata),
                                new ImplementFilteredAggregations(metadata)),
                        "TransformCorrelatedInPredicateToJoin"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new InlineProjections(),
                                new RemoveRedundantIdentityProjections(),
                                new TransformCorrelatedSingleRowSubqueryToProject(),
                                new RemoveAggregationInSemiJoin()),
                        "TransformCorrelatedSingleRowSubqueryToProject"),
                new CheckSubqueryNodesAreRewritten(),
                new PredicatePushDown(metadata, typeOperators, typeAnalyzer, false, false),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformFilteringSemiJoinToInnerJoin()),
                        "TransformFilteringSemiJoinToInnerJoin")); // must run after PredicatePushDown

        // Perform redirection before CBO rules to ensure stats from destination connector are used
        // Perform redirection before agg, topN, limit, sample etc. push down into table scan as the destination connector may support a different set of push downs
        // Perform redirection after at least one PredicatePushDown and PushPredicateIntoTableScan to allow connector to use pushed down predicates in redirection decision
        // Perform redirection after at least table scan pruning rules because redirected table might have fewer columns
        // PushPredicateIntoTableScan must be run after redirection
        // Column pruning rules need to be run after redirection
        builder.add(
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ApplyTableScanRedirection(metadata),
                                new PushProjectionIntoTableScan(metadata, typeAnalyzer),
                                new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer)),
                        "PushXXXIntoTableScan"));

        IterativeOptimizer pushIntoTableScanOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(columnPruningRules)
                        .addAll(projectionPushdownRules)
                        .add(new RemoveRedundantIdentityProjections())
                        .add(new PushLimitIntoTableScan(metadata))
                        .add(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer))
                        .add(new PushSampleIntoTableScan(metadata))
                        .add(new PushAggregationIntoTableScan(metadata))
                        .add(new PushDistinctLimitIntoTableScan(metadata))
                        .build(),
                "pushIntoTableScanOptimizer");
        builder.add(pushIntoTableScanOptimizer);
        builder.add(new UnaliasSymbolReferences(metadata));
        builder.add(pushIntoTableScanOptimizer); // TODO (https://github.com/trinodb/trino/issues/811) merge with the above after migrating UnaliasSymbolReferences to rules
        builder.add(
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        // Temporary hack: separate optimizer step to avoid the sample node being replaced by filter before pushing
                        // it to table scan node
                        ImmutableSet.of(new ImplementBernoulliSampleAsFilter(metadata)),
                        "ImplementBernoulliSampleAsFilter"),
                columnPruningOptimizer,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin(),
                                new RemoveRedundantCrossJoin()),
                        "RemoveRedundantIdentityProjections"), // Run this after PredicatePushDown optimizer as it inlines filter constants
                inlineProjections,
                simplifyOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                projectionPushDown,
                // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
                // pushdown into the connectors. We invoke PredicatePushdown and PushPredicateIntoTableScan after this
                // to leverage predicate pushdown on projected columns.
                new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, false),
                simplifyOptimizer,  // Should be always run after PredicatePushDown
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer)),
                        "PushPredicateIntoTableScan"),
                new UnaliasSymbolReferences(metadata), // Run again because predicate pushdown and projection pushdown might add more projections
                columnPruningOptimizer, // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata, typeOperators), // Run this after projections and filters have been fully simplified and pushed down
                new LimitPushDown(), // Run LimitPushDown before WindowFilterPushDown
                // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        SystemSessionProperties::useLegacyWindowFilterPushdown,
                        ImmutableList.of(new WindowFilterPushDown(metadata, typeOperators)),
                        ImmutableSet.of(
                                new PushdownLimitIntoRowNumber(),
                                new PushdownLimitIntoWindow(metadata),
                                new PushdownFilterIntoRowNumber(metadata, typeOperators),
                                new PushdownFilterIntoWindow(metadata, typeOperators),
                                new ReplaceWindowWithRowNumber(metadata)),
                        "PushdownXXXIntoRowNumber"),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                // add UnaliasSymbolReferences when it's ported
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .add(new PushPredicateThroughProjectIntoRowNumber(metadata, typeOperators))
                                .add(new PushPredicateThroughProjectIntoWindow(metadata, typeOperators))
                                .build(),
                        "PushPredicateThroughProjectIntoRowNumber"),
                inlineProjections,
                columnPruningOptimizer, // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections()),
                        "RemoveRedundantIdentityProjections"),
                new MetadataQueryOptimizer(metadata),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins(metadata)),
                        "EliminateCrossJoins"), // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
                new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, false),
                simplifyOptimizer, // Should be always run after PredicatePushDown
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer)),
                        "PushPredicateIntoTableScan"),
                projectionPushDown,
                // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
                // pushdown into the connectors. Invoke PredicatePushdown and PushPredicateIntoTableScan after this
                // to leverage predicate pushdown on projected columns.
                new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, false),
                simplifyOptimizer,  // Should be always run after PredicatePushDown
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer)),
                        "PushPredicateIntoTableScan"),
                columnPruningOptimizer,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections()),
                        "RemoveRedundantIdentityProjections"),

                // Because ReorderJoins runs only once,
                // PredicatePushDown, columnPruningOptimizer and RemoveRedundantIdentityProjections
                // need to run beforehand in order to produce an optimal join order
                // It also needs to run after EliminateCrossJoins so that its chosen order doesn't get undone.
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new ReorderJoins(metadata, costComparator)),
                        "ReorderJoins"));

        builder.add(new OptimizeMixedDistinctAggregations(metadata));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughProject(),
                        new PushTopNThroughOuterJoin(),
                        new PushTopNThroughUnion(),
                        new PushTopNIntoTableScan(metadata)),
                "PushTopNXXX"));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .addAll(new ExtractSpatialJoins(metadata, splitManager, pageSourceManager, typeAnalyzer).rules())
                        .add(new InlineProjections())
                        .build(),
                "ExtractSpatialJoins"));

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        // Must run before AddExchanges
                        new PushDeleteIntoConnector(metadata),
                        // Must run after join reordering because join reordering creates
                        // new join nodes without JoinNode.maySkipOutputDuplicates flag set
                        new OptimizeDuplicateInsensitiveJoins(metadata)),
                "OptimizeDuplicateInsensitiveJoins"));

        if (!forceSingleNode) {
            builder.add(new ReplicateSemiJoinInDelete()); // Must run before AddExchanges
            builder.add((new IterativeOptimizer(
                    ruleStats,
                    statsCalculator,
                    estimatedExchangesCostCalculator,
                    ImmutableSet.of(
                            new DetermineJoinDistributionType(costComparator, taskCountEstimator), // Must run before AddExchanges
                            // Must run before AddExchanges and after ReplicateSemiJoinInDelete
                            // to avoid temporarily having an invalid plan
                            new DetermineSemiJoinDistributionType(costComparator, taskCountEstimator)),
                    "DetermineJoinDistributionType")));
            builder.add(
                    new IterativeOptimizer(
                            ruleStats,
                            statsCalculator,
                            estimatedExchangesCostCalculator,
                            ImmutableSet.of(new PushTableWriteThroughUnion()),
                            "PushTableWriteThroughUnion")); // Must run before AddExchanges
            // unalias symbols before adding exchanges to use same partitioning symbols in joins, aggregations and other
            // operators that require node partitioning
            builder.add(new UnaliasSymbolReferences(metadata));
            builder.add(new AddExchanges(metadata, typeOperators, typeAnalyzer));
        }
        //noinspection UnusedAssignment
        estimatedExchangesCostCalculator = null; // Prevent accidental use after AddExchanges

        builder.add(
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveEmptyDelete()),
                        "RemoveEmptyDelete")); // Run RemoveEmptyDelete after table scan is removed by PickTableLayout/AddExchanges

        // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        // and to pushdown dynamic filters
        builder.add(
                new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, false));
        builder.add(simplifyOptimizer); // Should be always run after PredicatePushDown
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new RemoveRedundantTableScanPredicate(metadata, typeOperators)),
                "RemoveRedundantTableScanPredicate"));
        builder.add(projectionPushDown);
        // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
        // pushdown into the connectors. Invoke PredicatePushdown and PushPredicateIntoTableScan after this
        // to leverage predicate pushdown on projected columns.
        builder.add(new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, true));
        builder.add(new RemoveUnsupportedDynamicFilters(metadata)); // Remove unsupported dynamic filters introduced by PredicatePushdown
        builder.add(simplifyOptimizer); // Should always run after PredicatePushdown
        new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer)),
                "PushPredicateIntoTableScan");
        builder.add(inlineProjections);
        builder.add(new UnaliasSymbolReferences(metadata)); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(columnPruningOptimizer);

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .add(new PushRemoteExchangeThroughAssignUniqueId())
                        .add(new InlineProjections())
                        .build(),
                "PushRemoteExchangeThroughAssignUniqueId"));

        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(metadata, typeOperators, typeAnalyzer));

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new PushPartialAggregationThroughJoin(),
                        new PushPartialAggregationThroughExchange(metadata),
                        new PruneJoinColumns(),
                        new PruneJoinChildrenColumns()),
                "PushPartialAggregationThroughJoin"));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(metadata, typeOperators, typeAnalyzer, taskCountEstimator, taskManagerConfig).rules(), "AddExchangesBelowPartialAggregationOverGroupIdRuleSet"));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new AddIntermediateAggregations(),
                        new RemoveRedundantIdentityProjections()),
                "AddIntermediateAggregations"));
        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Remove any remaining sugar
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(new DesugarLike(metadata, typeAnalyzer).rules())
                        .addAll(new DesugarArrayConstructor(metadata, typeAnalyzer).rules())
                        .build(),
                "DesugarLike"));

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer(metadata));

        builder.add(new TableDeleteOptimizer(metadata));
        builder.add(new BeginTableWrite(metadata)); // HACK! see comments in BeginTableWrite

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    public List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
