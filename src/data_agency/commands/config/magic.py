from data_agency.common.llm_client import create_client, LLMModels


def config(line="", cell=""):
    client = create_client(model=LLMModels.GEMINI25_FLASH)
    cmd = line.strip() + " " + cell.strip()
    cmd = cmd.strip().lower()

    if cmd == "reset_api_usage":
        client.reset_usage()
        return "API usage has been reset."
    else:
        return "Unknown command. Available commands: reset_api_usage."
