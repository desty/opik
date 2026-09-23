package com.comet.opik.domain;

import com.comet.opik.api.resources.utils.ClickHouseContainerUtils;
import com.comet.opik.api.resources.utils.MigrationUtils;
import com.comet.opik.api.resources.utils.MySQLContainerUtils;
import com.comet.opik.api.resources.utils.RedisContainerUtils;
import com.comet.opik.api.resources.utils.TestDropwizardAppExtensionUtils;
import com.comet.opik.api.resources.utils.TestDropwizardAppExtensionUtils.AppContextConfig;
import com.comet.opik.api.resources.utils.TestDropwizardAppExtensionUtils.CustomConfig;
import com.comet.opik.extensions.DropwizardAppExtensionProvider;
import com.comet.opik.extensions.RegisterApp;
import com.comet.opik.infrastructure.auth.RequestContext;
import com.comet.opik.infrastructure.cache.CacheManager;
import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.mysql.MySQLContainer;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import ru.vyarus.dropwizard.guice.test.jupiter.ext.TestDropwizardAppExtension;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.comet.opik.api.resources.utils.ClickHouseContainerUtils.DATABASE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards that the {@code experiment_compare_target_projects} cache engages on the compare-view target-project
 * lookup, which doesn't depend on page or size. One read of a large experiment is dozens of requests differing
 * only by page, so losing the cache means running that query once per page.
 *
 * <p>The cache is seeded with a value the database cannot produce and the method is then checked to return it:
 * that proves the interceptor is wired (the method stays interceptable by Guice, the key expression evaluates,
 * and the declared return / wrapper types round-trip through JSON), without depending on any data being
 * present.
 *
 * <p>The second case is the one that matters most: the workspace id is part of the key, so the same scope in
 * another workspace must not see the seeded value.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DropwizardAppExtensionProvider.class)
class ExperimentCompareTargetProjectsCacheTest {

    private static final String CACHE_NAME = "experiment_compare_target_projects";
    private static final Duration CACHE_TTL = Duration.ofMinutes(10);
    private static final String USER = "user-" + UUID.randomUUID();

    private final RedisContainer REDIS = RedisContainerUtils.newRedisContainer();
    private final MySQLContainer MYSQL = MySQLContainerUtils.newMySQLContainer();
    private final GenericContainer<?> ZOOKEEPER_CONTAINER = ClickHouseContainerUtils.newZookeeperContainer();
    private final ClickHouseContainer CLICKHOUSE = ClickHouseContainerUtils.newClickHouseContainer(
            ZOOKEEPER_CONTAINER);

    @RegisterApp
    private final TestDropwizardAppExtension APP;

    {
        Startables.deepStart(MYSQL, CLICKHOUSE, REDIS, ZOOKEEPER_CONTAINER).join();

        var databaseAnalyticsFactory = ClickHouseContainerUtils.newDatabaseAnalyticsFactory(
                CLICKHOUSE, DATABASE_NAME);

        MigrationUtils.runMysqlDbMigration(MYSQL);
        MigrationUtils.runClickhouseDbMigration(CLICKHOUSE);

        APP = TestDropwizardAppExtensionUtils.newTestDropwizardAppExtension(
                AppContextConfig.builder()
                        .jdbcUrl(MYSQL.getJdbcUrl())
                        .databaseAnalyticsFactory(databaseAnalyticsFactory)
                        .redisUrl(REDIS.getRedisURI())
                        .customConfigs(List.of(
                                new CustomConfig("cacheManager.enabled", "true"),
                                new CustomConfig("cacheManager.caches." + CACHE_NAME,
                                        "PT%dS".formatted(CACHE_TTL.toSeconds()))))
                        .build());
    }

    @Test
    void getTargetProjectIds__servedFromCache(DatasetItemVersionDAO dao, CacheManager cacheManager) {
        var impl = (DatasetItemVersionDAOImpl) dao;
        var workspaceId = UUID.randomUUID().toString();
        var scopeKey = UUID.randomUUID().toString();
        var cachedProjectId = UUID.randomUUID();

        seed(workspaceId, scopeKey, List.of(cachedProjectId), cacheManager);

        var actual = block(impl.getTargetProjectIdsCached(workspaceId, scopeKey, UUID.randomUUID(), Set.of()),
                workspaceId);

        assertThat(actual).containsExactly(cachedProjectId);
    }

    @Test
    void cachedValues__areNotSharedAcrossWorkspaces(DatasetItemVersionDAO dao, CacheManager cacheManager) {
        var impl = (DatasetItemVersionDAOImpl) dao;
        var workspaceId = UUID.randomUUID().toString();
        var otherWorkspaceId = UUID.randomUUID().toString();
        var scopeKey = UUID.randomUUID().toString();

        seed(workspaceId, scopeKey, List.of(UUID.randomUUID()), cacheManager);

        var actual = block(impl.getTargetProjectIdsCached(otherWorkspaceId, scopeKey, UUID.randomUUID(), Set.of()),
                otherWorkspaceId);

        assertThat(actual).isEmpty();
    }

    private void seed(String workspaceId, String scopeKey, Object value, CacheManager cacheManager) {
        // CacheInterceptor composes "name:-" + the evaluated key expression.
        cacheManager.put("%s:-target_projects-%s-%s".formatted(CACHE_NAME, workspaceId, scopeKey), value, CACHE_TTL)
                .block();
    }

    private <T> T block(Mono<T> mono, String workspaceId) {
        return mono.contextWrite(Context.of(RequestContext.WORKSPACE_ID, workspaceId, RequestContext.USER_NAME, USER))
                .block();
    }
}
