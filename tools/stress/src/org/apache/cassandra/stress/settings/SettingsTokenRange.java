/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.stress.settings;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

public class SettingsTokenRange
{
    public final boolean wrap;
    public final int splitFactor;
    public final boolean prefetch;
    public final boolean stream;
    public final boolean localRouting;

    public SettingsTokenRange(TokenRangeOptions options)
    {
        this.wrap = options.wrap.setByUser();
        this.splitFactor = Ints.checkedCast(OptionDistribution.parseLong(options.splitFactor.value()));
        this.prefetch = options.prefetch.setByUser();
        this.stream = options.stream.setByUser();
        this.localRouting = options.localRouting.setByUser();

        if (this.prefetch && this.stream)
            throw new IllegalArgumentException("stream and prefetch cannot be both set, please remove one");
    }

    private static final class TokenRangeOptions extends GroupedOptions
    {
        final OptionSimple wrap = new OptionSimple("wrap", "", null, "Re-use token ranges in order to terminate stress iterations", false);
        final OptionSimple splitFactor = new OptionSimple("split-factor=", "[0-9]+[bmk]?", "1", "Split every token range by this factor", false);
        final OptionSimple prefetch = new OptionSimple("prefetch", "", null, "When processing a page result, pre-fetch the next page", false);
        final OptionSimple stream = new OptionSimple("stream", "", null, "Ask the server to stream all data available by splitting it into pages", false);
        final OptionSimple localRouting = new OptionSimple("localRouting", "", null, "Route queries to a local replica, ignored if a node white list is specified", false);


        @Override
        public List<? extends Option> options()
        {
            return ImmutableList.<Option>builder().add(wrap, splitFactor, prefetch, stream, localRouting).build();
        }
    }

    public static SettingsTokenRange get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-tokenrange");
        if (params == null)
        {
            return new SettingsTokenRange(new TokenRangeOptions());
        }
        TokenRangeOptions options = GroupedOptions.select(params, new TokenRangeOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -tokenrange options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsTokenRange(options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-tokenrange", new TokenRangeOptions());
    }

    public static Runnable helpPrinter()
    {
        return SettingsTokenRange::printHelp;
    }
}
