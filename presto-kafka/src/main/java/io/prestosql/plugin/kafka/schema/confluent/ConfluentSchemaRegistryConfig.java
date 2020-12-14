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
package io.prestosql.plugin.kafka.schema.confluent;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.prestosql.spi.HostAddress;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;

import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class ConfluentSchemaRegistryConfig
{
    private Set<HostAddress> confluentSchemaRegistryUrls;
    private int confluentSchemaRegistryClientCacheSize = 1000;

    @Size(min = 1)
    public Set<HostAddress> getConfluentSchemaRegistryUrls()
    {
        return confluentSchemaRegistryUrls;
    }

    @Config("kafka.confluent-schema-registry-url")
    @ConfigDescription("The url of the Confluent Schema Registry")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryUrls(String confluentSchemaRegistryUrls)
    {
        this.confluentSchemaRegistryUrls = (confluentSchemaRegistryUrls == null) ? null : parseNodes(confluentSchemaRegistryUrls);
        return this;
    }

    @Min(1)
    @Max(2000)
    public int getConfluentSchemaRegistryClientCacheSize()
    {
        return confluentSchemaRegistryClientCacheSize;
    }

    @Config("kafka.confluent-schema-registry-client-cache-size")
    @ConfigDescription("The maximum number of subjects that can be stored in the Confluent Schema Registry client cache")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryClientCacheSize(int confluentSchemaRegistryClientCacheSize)
    {
        this.confluentSchemaRegistryClientCacheSize = confluentSchemaRegistryClientCacheSize;
        return this;
    }

    private static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return StreamSupport.stream(splitter.split(nodes).spliterator(), false)
                .map(ConfluentSchemaRegistryConfig::toHostAddress)
                .collect(toImmutableSet());
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value);
    }
}
