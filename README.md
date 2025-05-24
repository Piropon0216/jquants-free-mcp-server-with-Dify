# J-Quants Free MCP server

[Model Context Protocol](https://modelcontextprotocol.io/introduction) (MCP) サーバーで、無償版J-Quants APIのにアクセスするための機能を提供します。

## ツール

このサーバーは以下のツールを提供しています：

- `search_company` : 日本語のテキストから、上場銘柄を検索する
- `get_daily_quotes` : 銘柄コードから、日次の株価を取得する
- `get_financial_statements` : 銘柄コードから、財務諸表を取得する


## 使い方
このサーバーを使用するには、J-Quants APIへの登録が必要です。以下の手順で取得できます：
- [J-Quants API](https://jpx-jquants.com/)に登録
- IDトークンを取得しして、`JQUANTS_ID_TOKEN`環境変数に設定


#### Claude Desktop

- On MacOS: `~/Library/Application\ Support/Claude/claude_desktop_config.json`
- On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

```json
{
    "mcpServers": {
        "e-stat": {
            "command": "uv",
            "args": [
                "--directory",
                "/path/to/jquants-free-mcp-server",
                "run",
                "server.py"
            ],
            "env": {
                "JQUANTS_ID_TOKEN": "YOUR_JQUANTS_ID_TOKEN"
            }
        }
    }
}
```

```json
{
    "mcpServers": {
        "e-stat": {
            "command": "uvx",
            "args": [
                "jquants-free-mcp-server"
            ],
            "env": {
                "JQUANTS_ID_TOKEN": "YOUR_JQUANTS_ID_TOKEN"
            }
        }
    }
}
```

## 使用例

例えばClaudeに以下のような質問ができます：
- "コメダとルノアールの自己資本比率を比較して"
- "UUUMとカバーとANYCOLORの財務表を取得して、バランスシートを図にしてください。"
![sample](https://github.com/user-attachments/assets/5e480007-228f-4ff9-a834-d79f490b3360)

## ライセンス

このプロジェクトはMITライセンスの下で提供されています
 - 詳細はLICENSEファイルを参照してください。
