package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	colorPrimary   = lipgloss.Color("#7C3AED")
	colorSecondary = lipgloss.Color("#06B6D4")
	colorSuccess   = lipgloss.Color("#22C55E")
	colorWarning   = lipgloss.Color("#F59E0B")
	colorError     = lipgloss.Color("#EF4444")
	colorMuted     = lipgloss.Color("#6B7280")
	colorText      = lipgloss.Color("#E5E7EB")
	colorBg        = lipgloss.Color("#111827")
	colorPanel     = lipgloss.Color("#1F2937")
	colorBorder    = lipgloss.Color("#374151")
	colorSelected  = lipgloss.Color("#312E81")
)

var (
	StyleTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorPrimary).
			MarginBottom(1)

	StyleSubtitle = lipgloss.NewStyle().
			Foreground(colorMuted)

	StyleHeader = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorSecondary).
			BorderStyle(lipgloss.NormalBorder()).
			BorderBottom(true).
			BorderForeground(colorBorder).
			Padding(0, 1)

	StylePanel = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Background(colorPanel).
			Padding(1, 2)

	StyleSidebar = lipgloss.NewStyle().
			Width(18).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Background(colorPanel).
			Padding(1, 1)

	StyleSidebarItem = lipgloss.NewStyle().
				Foreground(colorText).
				Padding(0, 1)

	StyleSidebarActive = lipgloss.NewStyle().
				Bold(true).
				Foreground(colorPrimary).
				Background(colorSelected).
				Padding(0, 1)

	StyleStatusBar = lipgloss.NewStyle().
			Foreground(colorMuted).
			Background(colorBg).
			Padding(0, 1)

	StyleError = lipgloss.NewStyle().
			Foreground(colorError).
			Bold(true)

	StyleSuccess = lipgloss.NewStyle().
			Foreground(colorSuccess)

	StyleWarning = lipgloss.NewStyle().
			Foreground(colorWarning)

	StyleKey = lipgloss.NewStyle().
			Foreground(colorSecondary).
			Bold(true)

	StyleValue = lipgloss.NewStyle().
			Foreground(colorText)

	StyleSelectedRow = lipgloss.NewStyle().
				Foreground(colorPrimary).
				Bold(true).
				Background(colorSelected)

	StyleRow = lipgloss.NewStyle().
			Foreground(colorText)

	StyleHelp = lipgloss.NewStyle().
			Foreground(colorMuted)
)

func RenderHeader(title, subtitle string) string {
	t := StyleTitle.Render("◆ " + title)
	if subtitle != "" {
		return t + "\n" + StyleSubtitle.Render(subtitle)
	}
	return t
}

func RenderStatusBar(left, right string) string {
	width := 80
	leftStyle := lipgloss.NewStyle().Foreground(colorMuted)
	rightStyle := lipgloss.NewStyle().Foreground(colorMuted)
	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		gap = 1
	}
	return leftStyle.Render(left) + strings.Repeat(" ", gap) + rightStyle.Render(right)
}

func Truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func FormatBrokers(brokers []string) string {
	return strings.Join(brokers, ", ")
}

func FormatOffset(first, last int64) string {
	if last <= first {
		return fmt.Sprintf("%d (empty)", first)
	}
	return fmt.Sprintf("%d → %d (%d msgs)", first, last, last-first)
}
