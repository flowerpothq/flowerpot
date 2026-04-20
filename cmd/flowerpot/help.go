package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func brandedHelp(cmd *cobra.Command, _ []string) {
	fmt.Println()
	fmt.Println(styleBrand.Render("  Flowerpot"))
	fmt.Println(styleDim.Render("  The data pipeline scheduler. One binary, zero dependencies."))
	fmt.Println()

	type group struct {
		title string
		cmds  []string
	}

	groups := []group{
		{"Commands", []string{"run", "validate", "status", "logs", "serve", "trigger"}},
		{"Setup", []string{"init", "version"}},
	}

	cmdMap := make(map[string]*cobra.Command)
	for _, c := range cmd.Commands() {
		cmdMap[c.Name()] = c
	}

	for _, g := range groups {
		hasAny := false
		for _, name := range g.cmds {
			if _, ok := cmdMap[name]; ok {
				hasAny = true
				break
			}
		}
		if !hasAny {
			continue
		}

		fmt.Println(styleBold.Render("  " + g.title))
		for _, name := range g.cmds {
			c, ok := cmdMap[name]
			if !ok {
				continue
			}
			padded := padRight(name, 14)
			fmt.Printf("    %s%s\n", styleBrand.Render(padded), styleDim.Render(c.Short))
		}
		fmt.Println()
	}

	fmt.Println(styleDim.Render("  Flags"))
	fmt.Println(styleDim.Render("    -h, --help      Show this help"))
	fmt.Println(styleDim.Render("    -v, --version   Print version"))
	fmt.Println()
	fmt.Println(styleDim.Render("  Run " + styleBrand.Render("flowerpot <command> --help") + styleDim.Render(" for details.")))
	fmt.Println()
}

func subcommandHelp(cmd *cobra.Command, _ []string) {
	fmt.Println()
	fmt.Printf("  %s %s\n", styleBrand.Render("flowerpot "+cmd.Name()), styleDim.Render("— "+cmd.Short))
	fmt.Println()

	if cmd.Long != "" {
		for _, line := range strings.Split(cmd.Long, "\n") {
			if strings.TrimSpace(line) == "" {
				fmt.Println()
			} else {
				fmt.Println(styleDim.Render("  " + line))
			}
		}
		fmt.Println()
	}

	fmt.Println(styleBold.Render("  Usage"))
	fmt.Printf("    %s\n", cmd.UseLine())
	fmt.Println()

	if cmd.HasAvailableLocalFlags() {
		fmt.Println(styleBold.Render("  Flags"))
		for _, line := range strings.Split(cmd.LocalFlags().FlagUsages(), "\n") {
			trimmed := strings.TrimRight(line, " ")
			if trimmed != "" {
				fmt.Println(styleDim.Render("  " + trimmed))
			}
		}
		fmt.Println()
	}

	if cmd.HasExample() {
		fmt.Println(styleBold.Render("  Examples"))
		for _, line := range strings.Split(cmd.Example, "\n") {
			fmt.Println("    " + line)
		}
		fmt.Println()
	}
}
