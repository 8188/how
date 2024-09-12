from src.utils import Tasks
import asyncio


async def main() -> None:
    T1 = Tasks("1")
    # T2 = Tasks('2')

    await asyncio.gather(
        T1.run(),
        # T2.run()
    )


if __name__ == "__main__":
    asyncio.run(main())
