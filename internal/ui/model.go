package ui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	kafkaclient "github.com/showx/kafkashow/internal/kafka"
)

type view int

const (
	viewConnect view = iota
	viewTopics
	viewTopicDetail
	viewMessages
	viewGroups
	viewGroupDetail
)

type keyMap struct {
	Up      key.Binding
	Down    key.Binding
	Enter   key.Binding
	Back    key.Binding
	Refresh key.Binding
	Messages key.Binding
	Tab     key.Binding
	Quit    key.Binding
	Help    key.Binding
}

func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Up, k.Down, k.Enter, k.Back, k.Refresh, k.Quit}
}

func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Up, k.Down, k.Enter, k.Back},
		{k.Refresh, k.Messages, k.Tab, k.Quit},
	}
}

var keys = keyMap{
	Up: key.NewBinding(
		key.WithKeys("up", "k"),
		key.WithHelp("↑/k", "上移"),
	),
	Down: key.NewBinding(
		key.WithKeys("down", "j"),
		key.WithHelp("↓/j", "下移"),
	),
	Enter: key.NewBinding(
		key.WithKeys("enter"),
		key.WithHelp("enter", "确认"),
	),
	Back: key.NewBinding(
		key.WithKeys("esc", "backspace"),
		key.WithHelp("esc", "返回"),
	),
	Refresh: key.NewBinding(
		key.WithKeys("r"),
		key.WithHelp("r", "刷新"),
	),
	Messages: key.NewBinding(
		key.WithKeys("m"),
		key.WithHelp("m", "消息"),
	),
	Tab: key.NewBinding(
		key.WithKeys("tab"),
		key.WithHelp("tab", "切换"),
	),
	Quit: key.NewBinding(
		key.WithKeys("q", "ctrl+c"),
		key.WithHelp("q", "退出"),
	),
	Help: key.NewBinding(
		key.WithKeys("?"),
		key.WithHelp("?", "帮助"),
	),
}

type errMsg struct{ err error }
type connectedMsg struct{ client *kafkaclient.Client }
type topicsLoadedMsg struct{ topics []kafkaclient.TopicInfo }
type partitionsLoadedMsg struct {
	topic      string
	partitions []kafkaclient.PartitionInfo
}
type messagesLoadedMsg struct {
	topic      string
	partition  int
	messages   []kafkaclient.Message
}
type groupsLoadedMsg struct{ groups []kafkaclient.GroupInfo }
type groupDetailLoadedMsg struct {
	groupID string
	state   string
	members []kafkaclient.GroupMember
}

type Model struct {
	width  int
	height int

	view       view
	keys       keyMap
	help       help.Model
	showHelp   bool
	spinner    spinner.Model
	loading    bool
	err        string

	brokerInput textinput.Model
	client      *kafkaclient.Client

	topics          []kafkaclient.TopicInfo
	topicCursor     int
	selectedTopic   string
	partitions      []kafkaclient.PartitionInfo
	partitionCursor int

	messages        []kafkaclient.Message
	messageCursor   int
	messagePartition int

	groups       []kafkaclient.GroupInfo
	groupCursor  int
	selectedGroup string
	groupState    string
	groupMembers []kafkaclient.GroupMember
	memberCursor int

	sidebarIndex int
}

func NewModel() Model {
	ti := textinput.New()
	ti.Placeholder = "localhost:9092"
	ti.Focus()
	ti.CharLimit = 256
	ti.Width = 50
	ti.Prompt = "Brokers: "

	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(colorSecondary)

	return Model{
		view:        viewConnect,
		keys:        keys,
		help:        help.New(),
		spinner:     s,
		brokerInput: ti,
		sidebarIndex: 0,
	}
}

func (m Model) Init() tea.Cmd {
	return textinput.Blink
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		if m.view == viewConnect {
			return m.updateConnect(msg)
		}
		if m.loading {
			switch {
			case key.Matches(msg, m.keys.Quit):
				return m, tea.Quit
			}
			return m, nil
		}

		switch {
		case key.Matches(msg, m.keys.Quit):
			return m, tea.Quit
		case key.Matches(msg, m.keys.Help):
			m.showHelp = !m.showHelp
			return m, nil
		}

		switch m.view {
		case viewTopics:
			return m.updateTopics(msg)
		case viewTopicDetail:
			return m.updateTopicDetail(msg)
		case viewMessages:
			return m.updateMessages(msg)
		case viewGroups:
			return m.updateGroups(msg)
		case viewGroupDetail:
			return m.updateGroupDetail(msg)
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case errMsg:
		m.loading = false
		m.err = msg.err.Error()
		return m, nil

	case connectedMsg:
		m.loading = false
		m.client = msg.client
		m.err = ""
		m.view = viewTopics
		m.sidebarIndex = 0
		return m, m.loadTopics()

	case topicsLoadedMsg:
		m.loading = false
		m.topics = msg.topics
		m.topicCursor = 0
		m.err = ""
		return m, nil

	case partitionsLoadedMsg:
		m.loading = false
		m.selectedTopic = msg.topic
		m.partitions = msg.partitions
		m.partitionCursor = 0
		m.view = viewTopicDetail
		m.err = ""
		return m, nil

	case messagesLoadedMsg:
		m.loading = false
		m.messages = msg.messages
		m.messageCursor = 0
		m.messagePartition = msg.partition
		m.view = viewMessages
		m.err = ""
		return m, nil

	case groupsLoadedMsg:
		m.loading = false
		m.groups = msg.groups
		m.groupCursor = 0
		m.err = ""
		return m, nil

	case groupDetailLoadedMsg:
		m.loading = false
		m.selectedGroup = msg.groupID
		m.groupState = msg.state
		m.groupMembers = msg.members
		m.memberCursor = 0
		m.view = viewGroupDetail
		m.err = ""
		return m, nil
	}

	return m, nil
}

func (m Model) updateConnect(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Quit):
		return m, tea.Quit
	case key.Matches(msg, m.keys.Enter):
		brokers := parseBrokers(m.brokerInput.Value())
		if len(brokers) == 0 {
			m.err = "请输入 broker 地址，例如 localhost:9092"
			return m, nil
		}
		m.loading = true
		m.err = ""
		return m, tea.Batch(m.spinner.Tick, m.connect(brokers))
	}

	var cmd tea.Cmd
	m.brokerInput, cmd = m.brokerInput.Update(msg)
	return m, cmd
}

func (m Model) updateTopics(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Up):
		if m.topicCursor > 0 {
			m.topicCursor--
		}
	case key.Matches(msg, m.keys.Down):
		if m.topicCursor < len(m.topics)-1 {
			m.topicCursor++
		}
	case key.Matches(msg, m.keys.Enter):
		if len(m.topics) > 0 {
			m.loading = true
			topic := m.topics[m.topicCursor].Name
			return m, tea.Batch(m.spinner.Tick, m.loadPartitions(topic))
		}
	case key.Matches(msg, m.keys.Refresh):
		m.loading = true
		return m, tea.Batch(m.spinner.Tick, m.loadTopics())
	case key.Matches(msg, m.keys.Tab):
		m.view = viewGroups
		m.sidebarIndex = 1
		if len(m.groups) == 0 {
			m.loading = true
			return m, tea.Batch(m.spinner.Tick, m.loadGroups())
		}
	}
	return m, nil
}

func (m Model) updateTopicDetail(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Back):
		m.view = viewTopics
	case key.Matches(msg, m.keys.Up):
		if m.partitionCursor > 0 {
			m.partitionCursor--
		}
	case key.Matches(msg, m.keys.Down):
		if m.partitionCursor < len(m.partitions)-1 {
			m.partitionCursor++
		}
	case key.Matches(msg, m.keys.Messages), key.Matches(msg, m.keys.Enter):
		if len(m.partitions) > 0 {
			m.loading = true
			partition := m.partitions[m.partitionCursor].ID
			return m, tea.Batch(m.spinner.Tick, m.loadMessages(m.selectedTopic, partition))
		}
	case key.Matches(msg, m.keys.Refresh):
		m.loading = true
		return m, tea.Batch(m.spinner.Tick, m.loadPartitions(m.selectedTopic))
	}
	return m, nil
}

func (m Model) updateMessages(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Back):
		m.view = viewTopicDetail
	case key.Matches(msg, m.keys.Up):
		if m.messageCursor > 0 {
			m.messageCursor--
		}
	case key.Matches(msg, m.keys.Down):
		if m.messageCursor < len(m.messages)-1 {
			m.messageCursor++
		}
	case key.Matches(msg, m.keys.Refresh):
		m.loading = true
		return m, tea.Batch(m.spinner.Tick, m.loadMessages(m.selectedTopic, m.messagePartition))
	}
	return m, nil
}

func (m Model) updateGroups(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Up):
		if m.groupCursor > 0 {
			m.groupCursor--
		}
	case key.Matches(msg, m.keys.Down):
		if m.groupCursor < len(m.groups)-1 {
			m.groupCursor++
		}
	case key.Matches(msg, m.keys.Enter):
		if len(m.groups) > 0 {
			m.loading = true
			groupID := m.groups[m.groupCursor].ID
			return m, tea.Batch(m.spinner.Tick, m.loadGroupDetail(groupID))
		}
	case key.Matches(msg, m.keys.Refresh):
		m.loading = true
		return m, tea.Batch(m.spinner.Tick, m.loadGroups())
	case key.Matches(msg, m.keys.Tab):
		m.view = viewTopics
		m.sidebarIndex = 0
		if len(m.topics) == 0 {
			m.loading = true
			return m, tea.Batch(m.spinner.Tick, m.loadTopics())
		}
	}
	return m, nil
}

func (m Model) updateGroupDetail(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Back):
		m.view = viewGroups
	case key.Matches(msg, m.keys.Up):
		if m.memberCursor > 0 {
			m.memberCursor--
		}
	case key.Matches(msg, m.keys.Down):
		if m.memberCursor < len(m.groupMembers)-1 {
			m.memberCursor++
		}
	case key.Matches(msg, m.keys.Refresh):
		m.loading = true
		return m, tea.Batch(m.spinner.Tick, m.loadGroupDetail(m.selectedGroup))
	}
	return m, nil
}

func (m Model) View() string {
	if m.width == 0 {
		return "加载中..."
	}

	switch m.view {
	case viewConnect:
		return m.viewConnect()
	default:
		return m.viewMain()
	}
}

func (m Model) viewConnect() string {
	var b strings.Builder
	b.WriteString(RenderHeader("kafkashow", "Kafka 终端查看工具"))
	b.WriteString("\n\n")
	b.WriteString(StylePanel.Render(
		m.brokerInput.View() + "\n\n" +
			StyleSubtitle.Render("多个 broker 用逗号分隔，例如: localhost:9092,localhost:9093") + "\n\n" +
			StyleHelp.Render("Enter 连接  •  Q 退出"),
	))
	if m.err != "" {
		b.WriteString("\n" + StyleError.Render("✗ "+m.err))
	}
	if m.loading {
		b.WriteString("\n\n" + m.spinner.View() + " 正在连接...")
	}
	return b.String()
}

func (m Model) viewMain() string {
	sidebar := m.renderSidebar()
	content := m.renderContent()
	helpBar := m.renderHelpBar()

	mainWidth := m.width - 22
	if mainWidth < 40 {
		mainWidth = 40
	}

	contentPanel := StylePanel.Width(mainWidth).Render(content)
	body := lipgloss.JoinHorizontal(lipgloss.Top, sidebar, contentPanel)

	var b strings.Builder
	b.WriteString(body)
	b.WriteString("\n")
	b.WriteString(helpBar)
	return b.String()
}

func (m Model) renderSidebar() string {
	items := []string{"Topics", "Groups"}
	var lines []string
	lines = append(lines, StyleHeader.Render("导航"))
	for i, item := range items {
		if i == m.sidebarIndex {
			lines = append(lines, StyleSidebarActive.Render("▸ "+item))
		} else {
			lines = append(lines, StyleSidebarItem.Render("  "+item))
		}
	}
	lines = append(lines, "")
	if m.client != nil {
		lines = append(lines, StyleSubtitle.Render("集群"))
		lines = append(lines, StyleSubtitle.Render(Truncate(FormatBrokers(m.client.Brokers()), 14)))
	}
	return StyleSidebar.Height(m.height - 4).Render(strings.Join(lines, "\n"))
}

func (m Model) renderContent() string {
	if m.loading {
		return RenderHeader("加载中", "") + "\n\n" + m.spinner.View() + " 请稍候..."
	}
	if m.err != "" {
		return RenderHeader("错误", "") + "\n\n" + StyleError.Render(m.err)
	}

	switch m.view {
	case viewTopics:
		return m.renderTopics()
	case viewTopicDetail:
		return m.renderTopicDetail()
	case viewMessages:
		return m.renderMessages()
	case viewGroups:
		return m.renderGroups()
	case viewGroupDetail:
		return m.renderGroupDetail()
	}
	return ""
}

func (m Model) renderTopics() string {
	var b strings.Builder
	b.WriteString(RenderHeader("Topics", fmt.Sprintf("共 %d 个 topic", len(m.topics))))
	b.WriteString("\n\n")

	if len(m.topics) == 0 {
		b.WriteString(StyleSubtitle.Render("暂无 topic，按 r 刷新"))
		return b.String()
	}

	header := fmt.Sprintf("%-40s %10s", "NAME", "PARTITIONS")
	b.WriteString(StyleHeader.Render(header))
	b.WriteString("\n")

	for i, t := range m.topics {
		row := fmt.Sprintf("%-40s %10d", Truncate(t.Name, 40), t.Partitions)
		if i == m.topicCursor {
			b.WriteString(StyleSelectedRow.Render("▸ " + row))
		} else {
			b.WriteString(StyleRow.Render("  " + row))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func (m Model) renderTopicDetail() string {
	var b strings.Builder
	b.WriteString(RenderHeader("Topic: "+m.selectedTopic, "分区详情"))
	b.WriteString("\n\n")

	header := fmt.Sprintf("%-6s %-8s %-20s %-30s", "PART", "LEADER", "OFFSETS", "REPLICAS")
	b.WriteString(StyleHeader.Render(header))
	b.WriteString("\n")

	for i, p := range m.partitions {
		offsets := FormatOffset(p.FirstOffset, p.LastOffset)
		replicas := fmt.Sprintf("%v", p.Replicas)
		row := fmt.Sprintf("%-6d %-8d %-20s %-30s", p.ID, p.Leader, Truncate(offsets, 20), Truncate(replicas, 30))
		if i == m.partitionCursor {
			b.WriteString(StyleSelectedRow.Render("▸ " + row))
		} else {
			b.WriteString(StyleRow.Render("  " + row))
		}
		b.WriteString("\n")
	}

	b.WriteString("\n" + StyleHelp.Render("Enter/m 查看消息  •  Esc 返回  •  r 刷新"))
	return b.String()
}

func (m Model) renderMessages() string {
	var b strings.Builder
	b.WriteString(RenderHeader(
		fmt.Sprintf("Messages: %s [%d]", m.selectedTopic, m.messagePartition),
		fmt.Sprintf("共 %d 条消息（最近 20 条）", len(m.messages)),
	))
	b.WriteString("\n\n")

	if len(m.messages) == 0 {
		b.WriteString(StyleSubtitle.Render("暂无消息"))
		return b.String()
	}

	for i, msg := range m.messages {
		isSelected := i == m.messageCursor
		prefix := "  "
		style := StyleRow
		if isSelected {
			prefix = "▸ "
			style = StyleSelectedRow
		}

		meta := fmt.Sprintf("%s offset=%d  time=%s",
			prefix,
			msg.Offset,
			msg.Time.Format("15:04:05"),
		)
		b.WriteString(style.Render(meta))
		b.WriteString("\n")

		if msg.Key != "" {
			b.WriteString(style.Render(fmt.Sprintf("    key: %s", Truncate(msg.Key, 60))))
			b.WriteString("\n")
		}
		b.WriteString(style.Render(fmt.Sprintf("    value: %s", Truncate(msg.Value, 80))))
		b.WriteString("\n\n")
	}

	b.WriteString(StyleHelp.Render("Esc 返回  •  r 刷新"))
	return b.String()
}

func (m Model) renderGroups() string {
	var b strings.Builder
	b.WriteString(RenderHeader("Consumer Groups", fmt.Sprintf("共 %d 个", len(m.groups))))
	b.WriteString("\n\n")

	if len(m.groups) == 0 {
		b.WriteString(StyleSubtitle.Render("暂无 consumer group，按 r 刷新"))
		return b.String()
	}

	header := fmt.Sprintf("%-40s %15s", "GROUP ID", "TYPE")
	b.WriteString(StyleHeader.Render(header))
	b.WriteString("\n")

	for i, g := range m.groups {
		row := fmt.Sprintf("%-40s %15s", Truncate(g.ID, 40), g.State)
		if i == m.groupCursor {
			b.WriteString(StyleSelectedRow.Render("▸ " + row))
		} else {
			b.WriteString(StyleRow.Render("  " + row))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func (m Model) renderGroupDetail() string {
	var b strings.Builder
	b.WriteString(RenderHeader("Group: "+m.selectedGroup, fmt.Sprintf("状态: %s  •  %d 个成员", m.groupState, len(m.groupMembers))))
	b.WriteString("\n\n")

	if len(m.groupMembers) == 0 {
		b.WriteString(StyleSubtitle.Render("暂无活跃成员"))
		return b.String()
	}

	header := fmt.Sprintf("%-20s %-20s %-30s", "MEMBER ID", "CLIENT ID", "HOST")
	b.WriteString(StyleHeader.Render(header))
	b.WriteString("\n")

	for i, member := range m.groupMembers {
		row := fmt.Sprintf("%-20s %-20s %-30s",
			Truncate(member.ID, 20),
			Truncate(member.ClientID, 20),
			Truncate(member.Host, 30),
		)
		if i == m.memberCursor {
			b.WriteString(StyleSelectedRow.Render("▸ " + row))
		} else {
			b.WriteString(StyleRow.Render("  " + row))
		}
		b.WriteString("\n")
	}

	b.WriteString("\n" + StyleHelp.Render("Esc 返回  •  r 刷新"))
	return b.String()
}

func (m Model) renderHelpBar() string {
	if m.showHelp {
		return m.help.View(m.keys)
	}
	left := "kafkashow"
	right := "?:帮助  tab:切换  r:刷新  q:退出"
	return StyleStatusBar.Width(m.width).Render(RenderStatusBar(left, right))
}

func parseBrokers(input string) []string {
	parts := strings.Split(input, ",")
	var brokers []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			brokers = append(brokers, p)
		}
	}
	return brokers
}

func (m Model) connect(brokers []string) tea.Cmd {
	return func() tea.Msg {
		client, err := kafkaclient.New(brokers)
		if err != nil {
			return errMsg{err}
		}
		return connectedMsg{client}
	}
}

func (m Model) loadTopics() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		topics, err := m.client.ListTopics(ctx)
		if err != nil {
			return errMsg{err}
		}
		return topicsLoadedMsg{topics}
	}
}

func (m Model) loadPartitions(topic string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		partitions, err := m.client.GetTopicPartitions(ctx, topic)
		if err != nil {
			return errMsg{err}
		}
		return partitionsLoadedMsg{topic: topic, partitions: partitions}
	}
}

func (m Model) loadMessages(topic string, partition int) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		messages, err := m.client.ReadMessages(ctx, topic, partition, -1, 20)
		if err != nil {
			return errMsg{err}
		}
		return messagesLoadedMsg{topic: topic, partition: partition, messages: messages}
	}
}

func (m Model) loadGroups() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		groups, err := m.client.ListGroups(ctx)
		if err != nil {
			return errMsg{err}
		}
		return groupsLoadedMsg{groups}
	}
}

func (m Model) loadGroupDetail(groupID string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		state, members, err := m.client.DescribeGroup(ctx, groupID)
		if err != nil {
			return errMsg{err}
		}
		return groupDetailLoadedMsg{groupID: groupID, state: state, members: members}
	}
}
