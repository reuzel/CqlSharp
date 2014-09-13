// CqlSharp - CqlSharp.Performance.Client
// Copyright (c) 2014 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Linq;
using CommandLine;
using CommandLine.Text;

namespace CqlSharp.Performance.Client
{
    internal class Options
    {
        [ParserState]
        public IParserState LastParserState { get; set; }

        [Option('c', "concurrent", DefaultValue = 25, HelpText = "Number of concurrent requests")]
        public int Concurrent { get; set; }

        [Option('r', "requests", DefaultValue = 10000, HelpText = "Number of requests to send")]
        public int Requests { get; set; }

        [Option('s', "sync", DefaultValue = false, HelpText = "Use synchronous API")]
        public bool Sync { get; set; }

        [Option('h', "host", DefaultValue = "http://localhost", HelpText = "Host/Server name where the API is deployed")
        ]
        public string Server { get; set; }


        [HelpOption]
        public string GetUsage()
        {
            var help = new HelpText
            {
                Heading = new HeadingInfo("CqlSharp.Performance.Client", "0.1.0"),
                Copyright = new CopyrightInfo("Joost Reuzel", 2014),
                AdditionalNewLineAfterOption = true,
                AddDashesToOption = true
            };

            if(LastParserState.Errors.Any())
            {
                var errors = help.RenderParsingErrorsText(this, 2); // indent with two spaces

                if(!string.IsNullOrEmpty(errors))
                {
                    help.AddPreOptionsLine(string.Concat(Environment.NewLine, "ERROR(S):"));
                    help.AddPreOptionsLine(errors);
                }
            }

            help.AddPreOptionsLine("Usage: Client -c 100 -r 10000");
            help.AddOptions(this);
            return help;
        }
    }
}