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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.optimizations.PruneUnreferencedOutputs;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.testing.LocalQueryRunner;
import io.trino.tpch.Customer;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TestTpchPlan
{
    private final Map<String, String> sessionProperties;
    private LocalQueryRunner queryRunner;
    private List<String> queries;

    public TestTpchPlan()
    {
        this(ImmutableMap.of());
    }

    public TestTpchPlan(Map<String, String> sessionProperties)
    {
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    // Subclasses should implement this method to inject their own query runners
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf1")
                .setSystemProperty("io.trino.testng.services.FlakyTestRetryAnalyzer.enabled", "true")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        sessionProperties.entrySet().forEach(entry -> sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue()));

        LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        return queryRunner;
    }

    @BeforeClass
    public final void initPlanTest()
    {
        this.queryRunner = createLocalQueryRunner();
        this.queries = IntStream.rangeClosed(1, 22)
                .boxed()
                .filter(i -> i != 15) // q15 has two queries in it
                .map(i -> readResource(format("/io/trino/tpch/queries/q%d.sql", i)))
                .collect(toImmutableList());
    }

    @AfterClass(alwaysRun = true)
    public final void destroyPlanTest()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void test()
    {
        String sql = "explain select * from customer";
        subplan(sql, OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ1()
    {
        subplan(queries.get(0), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ2()
    {
        subplan(queries.get(1), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ3()
    {
        subplan(queries.get(2), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ4()
    {
        subplan(queries.get(3), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ5()
    {
        subplan(queries.get(4), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ6()
    {
        subplan(queries.get(5), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ7()
    {
        subplan(queries.get(6), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ8()
    {
        subplan(queries.get(7), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ9()
    {
        subplan(queries.get(8), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ10()
    {
        subplan(queries.get(9), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ11()
    {
        subplan(queries.get(10), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ12()
    {
        subplan(queries.get(11), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ13()
    {
        subplan(queries.get(12), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ14()
    {
        subplan(queries.get(13), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ15()
    {
        subplan(queries.get(14), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ16()
    {
        subplan(queries.get(15), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ17()
    {
        subplan(queries.get(16), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ18()
    {
        subplan(queries.get(17), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ19()
    {
        subplan(queries.get(18), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ20()
    {
        subplan(queries.get(19), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ21()
    {
        subplan(queries.get(20), OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testQ22()
    {
        subplan(queries.get(21), OPTIMIZED_AND_VALIDATED, false);
    }

    // Non correlated InPredicate
    @Test
    public void testNonCorrelatedEqualPredicate()
    {
        String sql = "SELECT *\n" +
                "FROM nation\n" +
                "WHERE n_nationkey = (\n" +
                "  SELECT n_nationkey FROM nation WHERE n_name = 'ALGERIA' LIMIT 1\n" +
                ")";
        subplan(sql, OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testNonCorrelatedNotEqualPredicate()
    {
        String sql = "SELECT *\n" +
                "FROM nation\n" +
                "WHERE n_nationkey != (\n" +
                "  SELECT n_nationkey FROM nation WHERE n_name = 'ALGERIA' LIMIT 1\n" +
                ")";
        subplan(sql, OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testNonCorrelatedInPredicate()
    {
        String sql = "SELECT *\n" +
                "FROM nation\n" +
                "WHERE n_nationkey in (\n" +
                "  SELECT n_nationkey FROM nation WHERE n_name = 'ALGERIA' LIMIT 1\n" +
                ")";
        subplan(sql, OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testNonCorrelatedNotInPredicate()
    {
        String sql = "SELECT *\n" +
                "FROM nation\n" +
                "WHERE n_nationkey not in (\n" +
                "  SELECT n_nationkey FROM nation WHERE n_name = 'ALGERIA' LIMIT 1\n" +
                ")";
        subplan(sql, OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testCorrelatedExistsPredicate()
    {
        String sql = "SELECT c_nationkey\n" +
                "FROM customer \n" +
                "WHERE exists (\n" +
                "  SELECT n_nationkey FROM nation WHERE c_nationkey = n_nationkey and n_name = 'ALGERIA' LIMIT 1\n" +
                ")";
        subplan(sql, OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testCorrelatedNotExistsPredicate()
    {
        String sql = "SELECT c_nationkey\n" +
                "FROM customer \n" +
                "WHERE not exists (\n" +
                "  SELECT n_nationkey FROM nation WHERE c_nationkey = n_nationkey and n_name = 'ALGERIA' LIMIT 1\n" +
                ")";
        subplan(sql, OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testCorrelatedSarlarAggSubquery()
    {
        String sql = "SELECT c_nationkey\n" +
                "FROM customer \n" +
                "WHERE 10 < (\n" +
                "  SELECT min(n_regionkey) FROM nation WHERE c_nationkey = n_nationkey and n_name = 'ALGERIA' LIMIT 1\n" +
                ")";
        subplan(sql, OPTIMIZED_AND_VALIDATED, false);
    }

    private String readResource(String resource)
    {
        try {
            URL resourceUrl = Customer.class.getResource(resource);
            return Resources.toString(resourceUrl, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected CatalogName getCurrentConnectorId()
    {
        return queryRunner.inTransaction(transactionSession -> queryRunner.getMetadata().getCatalogHandle(transactionSession, transactionSession.getCatalog().get())).get();
    }

    protected LocalQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    protected void assertPlan(String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, OPTIMIZED_AND_VALIDATED, pattern);
    }

    protected void assertPlan(String sql, Session session, PlanMatchPattern pattern)
    {
        assertPlanWithSession(sql, session, true, pattern);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(String sql, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        assertPlan(sql, OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, Predicate<PlanOptimizer> optimizerPredicate)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true).stream()
                .filter(optimizerPredicate)
                .collect(toList());

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers, stage, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertDistributedPlan(String sql, PlanMatchPattern pattern)
    {
        assertDistributedPlan(sql, getQueryRunner().getDefaultSession(), pattern);
    }

    protected void assertDistributedPlan(String sql, Session session, PlanMatchPattern pattern)
    {
        assertPlanWithSession(sql, session, false, pattern);
    }

    protected void assertMinimallyOptimizedPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(getQueryRunner().getMetadata()),
                new PruneUnreferencedOutputs(queryRunner.getMetadata()),
                new IterativeOptimizer(
                        new RuleStatsRecorder(),
                        queryRunner.getStatsCalculator(),
                        queryRunner.getCostCalculator(),
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())));

        assertPlan(sql, OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, OPTIMIZED_AND_VALIDATED, forceSingleNode, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern, Consumer<Plan> planValidator)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, OPTIMIZED_AND_VALIDATED, forceSingleNode, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            planValidator.accept(actualPlan);
            return null;
        });
    }

    protected Plan plan(String sql)
    {
        return plan(sql, OPTIMIZED_AND_VALIDATED);
    }

    protected Plan plan(String sql, LogicalPlanner.Stage stage)
    {
        return plan(sql, stage, true);
    }

    protected Plan plan(String sql, LogicalPlanner.Stage stage, boolean forceSingleNode)
    {
        try {
            return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, stage, forceSingleNode, WarningCollector.NOOP));
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    protected SubPlan subplan(String sql, LogicalPlanner.Stage stage, boolean forceSingleNode)
    {
        return subplan(sql, stage, forceSingleNode, getQueryRunner().getDefaultSession());
    }

    protected SubPlan subplan(String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, Session session)
    {
        try {
            return queryRunner.inTransaction(session, transactionSession -> {
                Plan plan = queryRunner.createPlan(transactionSession, sql, stage, forceSingleNode, WarningCollector.NOOP);
                return queryRunner.createSubPlans(transactionSession, plan, forceSingleNode);
            });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }
}
