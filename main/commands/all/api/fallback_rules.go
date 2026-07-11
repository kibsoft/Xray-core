package api

import (
	"fmt"

	routerService "github.com/xtls/xray-core/app/router/command"
	cserial "github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/infra/conf/serial"
	"github.com/xtls/xray-core/main/commands/base"
)

var cmdAddFallbackRules = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api adfbrules [--server=127.0.0.1:8080] <c1.json> [c2.json]...",
	Short:       "Add fallback routing rules",
	Long: `
Add fallback routing rules to Xray.

Arguments:
	<c1.json> [c2.json]...
		The configs with fallback rules to be added. Must contain a "routing" field.

	-s, -server <server:port>
		The API server address. Default 127.0.0.1:8080

	-t, -timeout <seconds>
		Timeout seconds to call API. Default 3

	-append
		Append to the existing fallback rules instead of replacing them.
`,
	Run: executeAddFallbackRules,
}

func executeAddFallbackRules(cmd *base.Command, args []string) {
	var shouldAppend bool
	setSharedFlags(cmd)
	cmd.Flag.BoolVar(&shouldAppend, "append", false, "")
	cmd.Flag.Parse(args)

	unnamedArgs := cmd.Flag.Args()
	if len(unnamedArgs) == 0 {
		fmt.Println("reading from stdin:")
		unnamedArgs = []string{"stdin:"}
	}
	conn, ctx, close := dialAPIServer()
	defer close()

	client := routerService.NewRoutingServiceClient(conn)

	for _, arg := range unnamedArgs {
		r, err := loadArg(arg)
		if err != nil {
			base.Fatalf("failed to load %s: %s", arg, err)
		}
		conf, err := serial.DecodeJSONConfig(r)
		if err != nil {
			base.Fatalf("failed to decode %s: %s", arg, err)
		}
		if conf.RouterConfig == nil {
			base.Fatalf("failed to add fallback rules: config did not have \"routing\" field")
		}
		rcs := *conf.RouterConfig
		config, err := rcs.Build()
		if err != nil {
			base.Fatalf("failed to build conf: %s", err)
		}
		tmsg := cserial.ToTypedMessage(config)
		if tmsg == nil {
			base.Fatalf("failed to format config to TypedMessage.")
		}
		ra := &routerService.AddFallbackRuleRequest{
			Config:       tmsg,
			ShouldAppend: shouldAppend,
		}
		resp, err := client.AddFallbackRule(ctx, ra)
		if err != nil {
			base.Fatalf("failed to perform AddFallbackRule: %s", err)
		}
		showJSONResponse(resp)
	}
}

var cmdRemoveFallbackRules = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api rmfbrules [--server=127.0.0.1:8080] [ruleTag]...",
	Short:       "Remove fallback routing rules by ruleTag",
	Long: `
Remove fallback routing rules by ruleTag from Xray.
`,
	Run: executeRemoveFallbackRules,
}

func executeRemoveFallbackRules(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	cmd.Flag.Parse(args)
	ruleTags := cmd.Flag.Args()
	if len(ruleTags) == 0 {
		fmt.Println("reading from stdin:")
		ruleTags = []string{"stdin:"}
	}
	conn, ctx, close := dialAPIServer()
	defer close()

	client := routerService.NewRoutingServiceClient(conn)
	for _, tag := range ruleTags {
		rr := &routerService.RemoveFallbackRuleRequest{RuleTag: tag}
		resp, err := client.RemoveFallbackRule(ctx, rr)
		if err != nil {
			base.Fatalf("failed to perform RemoveFallbackRule: %s", err)
		}
		showJSONResponse(resp)
	}
}

var cmdRoutingMode = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api routingmode [--server=127.0.0.1:8080]",
	Short:       "Get current routing mode",
	Long: `
Get whether Xray is currently in fallback routing mode.
`,
	Run: executeRoutingMode,
}

func executeRoutingMode(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	cmd.Flag.Parse(args)
	conn, ctx, close := dialAPIServer()
	defer close()

	client := routerService.NewRoutingServiceClient(conn)
	resp, err := client.GetRoutingMode(ctx, &routerService.GetRoutingModeRequest{})
	if err != nil {
		base.Fatalf("failed to perform GetRoutingMode: %s", err)
	}
	showJSONResponse(resp)
}
