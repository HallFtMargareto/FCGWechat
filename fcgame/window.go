package fcgame

import (
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// 定义全局变量以便在函数间共享
var (
	app     *tview.Application
	output  *tview.TextView
	input   *tview.InputField
	manager *Manager
)

// SendToOutput 发送字符串到输出区域
func SendToOutput(text string) {
	manager.Send(text)
	if output != nil {
		lines := strings.Split(output.GetText(false), "\n")
		lines = append(lines, text)
		if len(lines) > 2000 { // 保留最新 2000 行
			lines = lines[len(lines)-2000:]
		}
		output.SetText(strings.Join(lines, "\n"))
		output.ScrollToEnd()
	}
}

// CreateApp 创建应用程序界面
func CreateApp(m *Manager) {
	manager = m

	app = tview.NewApplication()

	// 输出区域
	output = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetChangedFunc(func() {
			app.Draw()
		})
	output.SetBorder(true).SetTitle("控制台输出")

	// 输入区域
	input = tview.NewInputField().
		SetLabel("输入指令: ").
		SetFieldWidth(100).
		SetFieldBackgroundColor(tcell.ColorDefault). // 改为默认背景
		SetDoneFunc(func(key tcell.Key) {
			if key == tcell.KeyEnter {
				// text := input.GetText()
				text := strings.TrimSpace(input.GetText())
				if text != "" {
					SendToOutput(text)
					input.SetText("")
				}
			}
		})
	input.SetBorder(true).SetTitle("用户输入")

	// 上下布局
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(output, 0, 1, false).
		AddItem(input, 5, 1, true)

	// 最外层大盒子（相当于你原来那样的 Box）
	box := tview.NewFrame(flex).
		SetBorders(1, 1, 1, 1, 2, 2) // 边框样式
	box.SetTitle("FCGame").SetBorder(true)

	if err := app.SetRoot(box, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}
