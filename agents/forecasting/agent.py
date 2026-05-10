"""Stub agent — placeholder until implemented."""
from common.agents_base import BaseAgent


class ForecastingAgent(BaseAgent):
    name = "forecasting"

    async def setup(self) -> None:
        self.log.info("Stub agent started")

    def subscriptions(self):
        return {}


if __name__ == "__main__":
    import asyncio
    asyncio.run(ForecastingAgent().run())
