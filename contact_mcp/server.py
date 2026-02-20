from .tools import mcp


def run(transport: str = "stdio") -> None:
    mcp.run(transport=transport)


def main() -> None:
    run()


if __name__ == "__main__":
    main()
