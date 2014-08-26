using System;
using System.Linq;
using CommandLine;
using CommandLine.Text;

namespace CqlSharp.Performance.Client
{
    class Options
    {
        [ParserState]
        public IParserState LastParserState { get; set; }
        
        [Option('c', "concurrent", DefaultValue = 25, HelpText = "Number of concurrent requests")]
        public int Concurrent { get; set; }

        [Option('r', "requests", DefaultValue = 10000, HelpText = "Number of requests to send")]
        public int Requests { get; set; }

        [Option('s', "sync", DefaultValue = false, HelpText = "Use synchronous API")]
        public bool Sync { get; set; }

        [Option('h', "host", DefaultValue = "localhost", HelpText = "Host/Server name where the API is deployed")]
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

            if (LastParserState.Errors.Any())
            {
                var errors = help.RenderParsingErrorsText(this, 2); // indent with two spaces

                if (!string.IsNullOrEmpty(errors))
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