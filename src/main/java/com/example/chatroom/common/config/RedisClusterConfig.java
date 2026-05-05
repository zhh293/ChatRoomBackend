package com.example.chatroom.common.config;

/*
 * ============================================================
 * 【Redis Cluster 预留配置】
 *
 * 当前未启用，整个文件处于注释状态，开箱即用。
 *
 * 启用步骤（与 yml / pom 联动）：
 *   1. 取消 pom.xml 中 commons-pool2 依赖的注释
 *   2. 取消 application-dev.yml / application-prod.yml 中 Cluster 配置块的注释，
 *      同时注释掉原 host/port 单机/主从配置
 *   3. 取消本文件中所有 Java 注释（即删除每行开头的 " * "，恢复为正常 Java 代码）
 *
 * 为什么需要这个配置类？
 *   Spring Boot 自动装配的 RedisTemplate<Object, Object> 使用 JDK 序列化，
 *   业务代码注入的是 RedisTemplate<String, Object>（JSON 序列化）和
 *   StringRedisTemplate，需要手动声明 Bean 覆盖默认行为。
 *   Cluster 模式下还需要显式配置 ClusterTopologyRefreshOptions 以支持
 *   自适应拓扑刷新（节点故障/扩缩容时自动感知），Spring Boot 自动装配不会
 *   自动开启该选项。
 * ============================================================
 *
 * import com.fasterxml.jackson.annotation.JsonAutoDetect;
 * import com.fasterxml.jackson.annotation.PropertyAccessor;
 * import com.fasterxml.jackson.databind.ObjectMapper;
 * import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
 * import io.lettuce.core.cluster.ClusterClientOptions;
 * import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
 * import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
 * import org.springframework.context.annotation.Bean;
 * import org.springframework.context.annotation.Configuration;
 * import org.springframework.data.redis.connection.RedisClusterConfiguration;
 * import org.springframework.data.redis.connection.RedisConnectionFactory;
 * import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
 * import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
 * import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
 * import org.springframework.data.redis.core.RedisTemplate;
 * import org.springframework.data.redis.core.StringRedisTemplate;
 * import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
 * import org.springframework.data.redis.serializer.StringRedisSerializer;
 * import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
 *
 * import java.time.Duration;
 *
 * @Configuration
 * public class RedisClusterConfig {
 *
 *     /**
 *      * Lettuce Cluster 连接工厂
 *      *
 *      * <p>核心配置：
 *      * <ul>
 *      *   <li>自适应拓扑刷新（MOVED_REDIRECT + PERSISTENT_RECONNECTS）：
 *      *       节点故障或扩缩容时自动更新路由表，无需重启服务</li>
 *      *   <li>定期刷新兜底（30s）：防止自适应刷新遗漏边缘场景</li>
 *      *   <li>连接池：max-active=50，与 yml 中 pool 配置保持一致</li>
 *      * </ul>
 *      *
 *      * 注意：RedisProperties 由 Spring Boot 自动绑定 yml 中的
 *      * spring.data.redis.cluster.nodes / password / lettuce.pool 等配置。
 *      *\/
 *     @Bean
 *     public RedisConnectionFactory redisConnectionFactory(RedisProperties redisProperties) {
 *         // ① Cluster 节点 + 最大重定向次数
 *         RedisClusterConfiguration clusterConfig =
 *                 new RedisClusterConfiguration(redisProperties.getCluster().getNodes());
 *         clusterConfig.setMaxRedirects(redisProperties.getCluster().getMaxRedirects());
 *         if (redisProperties.getPassword() != null && !redisProperties.getPassword().isEmpty()) {
 *             clusterConfig.setPassword(redisProperties.getPassword());
 *         }
 *
 *         // ② 自适应拓扑刷新选项
 *         ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
 *                 // 触发自适应刷新的事件：MOVED 重定向、持续重连失败
 *                 .enableAdaptiveRefreshTrigger(
 *                         ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT,
 *                         ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
 *                 // 定期刷新兜底，防止自适应刷新遗漏
 *                 .enablePeriodicRefresh(Duration.ofSeconds(30))
 *                 .build();
 *
 *         ClusterClientOptions clientOptions = ClusterClientOptions.builder()
 *                 .topologyRefreshOptions(topologyRefreshOptions)
 *                 .build();
 *
 *         // ③ 连接池配置（对应 yml 中 lettuce.pool.*）
 *         RedisProperties.Pool poolProps = redisProperties.getLettuce().getPool();
 *         GenericObjectPoolConfig<?> poolConfig = new GenericObjectPoolConfig<>();
 *         poolConfig.setMaxTotal(poolProps.getMaxActive());
 *         poolConfig.setMaxIdle(poolProps.getMaxIdle());
 *         poolConfig.setMinIdle(poolProps.getMinIdle());
 *         if (poolProps.getMaxWait() != null) {
 *             poolConfig.setMaxWait(poolProps.getMaxWait());
 *         }
 *
 *         // ④ 组装 Lettuce 客户端配置
 *         LettuceClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
 *                 .clientOptions(clientOptions)
 *                 .poolConfig(poolConfig)
 *                 .build();
 *
 *         return new LettuceConnectionFactory(clusterConfig, clientConfig);
 *     }
 *
 *     /**
 *      * RedisTemplate<String, Object>：key 用 String 序列化，value 用 JSON 序列化
 *      *
 *      * <p>业务代码中注入 RedisTemplate<String, Object> 的地方（如 ChatWebSocketHandler、
 *      * SessionCacheService 等）均依赖此 Bean，序列化方式与单机模式保持一致，
 *      * 切换 Cluster 后业务代码无需任何改动。
 *      *\/
 *     @Bean
 *     public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
 *         RedisTemplate<String, Object> template = new RedisTemplate<>();
 *         template.setConnectionFactory(connectionFactory);
 *
 *         // Key / HashKey 使用 String 序列化
 *         StringRedisSerializer stringSerializer = new StringRedisSerializer();
 *         template.setKeySerializer(stringSerializer);
 *         template.setHashKeySerializer(stringSerializer);
 *
 *         // Value / HashValue 使用 Jackson JSON 序列化，并写入类型信息（反序列化时还原具体类型）
 *         ObjectMapper om = new ObjectMapper();
 *         om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
 *         om.activateDefaultTyping(
 *                 LaissezFaireSubTypeValidator.instance,
 *                 ObjectMapper.DefaultTyping.NON_FINAL);
 *         Jackson2JsonRedisSerializer<Object> jsonSerializer =
 *                 new Jackson2JsonRedisSerializer<>(om, Object.class);
 *         template.setValueSerializer(jsonSerializer);
 *         template.setHashValueSerializer(jsonSerializer);
 *
 *         template.afterPropertiesSet();
 *         return template;
 *     }
 *
 *     /**
 *      * StringRedisTemplate：key/value 均为纯字符串，供 MessageCacheService 等使用
 *      *\/
 *     @Bean
 *     public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
 *         return new StringRedisTemplate(connectionFactory);
 *     }
 * }
 *
 * ============================================================
 * 【Redis Cluster 预留 END】
 * ============================================================
 */
public class RedisClusterConfig {
    // 占位类，保持文件可编译。启用时删除此空类，取消上方注释即可。
}
