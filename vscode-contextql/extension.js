const { LanguageClient, TransportKind } = require("vscode-languageclient/node");

let client;

function activate(context) {
    const serverOptions = {
        command: "contextql-lsp",
        transport: TransportKind.stdio,
    };

    const clientOptions = {
        documentSelector: [{ scheme: "file", language: "contextql" }],
    };

    client = new LanguageClient(
        "contextql",
        "ContextQL Language Server",
        serverOptions,
        clientOptions
    );

    client.start();
}

function deactivate() {
    if (client) {
        return client.stop();
    }
}

module.exports = { activate, deactivate };
