import os
import datetime
from enum import Enum
from typing import Sequence

from diskcache import Cache
from autogen_ext.cache_store.diskcache import DiskCacheStore
from autogen_ext.models.cache import ChatCompletionCache, CHAT_CACHE_VALUE_TYPE
from autogen_core.models import UserMessage, ModelInfo, CreateResult, LLMMessage
from autogen_ext.models.openai import OpenAIChatCompletionClient
from .mylog import get_logger

from .load_env import CACHE_PATH

# Constants
GOOGLE_OPENAI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
MAX_TOTAL_CALLS = 1000
MAX_TOTAL_TOKENS = 1_000_000
DEBUG = False


class LLMModels(Enum):
    GEMINI25_FLASH = "gemini-2.5-flash"
    GEMINI25_FLASH_LITE = "gemini-2.5-flash-lite-preview-06-17"
    GEMINI25_PRO = "gemini-2.5-pro"
    OPENAI_GPT_4 = "gpt-4"
    OPENAI_GPT_3_5 = "gpt-3.5-turbo"


class UsageLimitExceededError(Exception):
    pass


class GeminiAPIError(Exception):
    pass


class ModelClientFactory:
    @staticmethod
    def create_client(model: LLMModels = LLMModels.GEMINI25_FLASH):
        if model in {LLMModels.GEMINI25_FLASH, LLMModels.GEMINI25_FLASH_LITE, LLMModels.GEMINI25_PRO}:
            return ModelClientFactory._create_gemini_client(model)
        elif model in {LLMModels.OPENAI_GPT_4, LLMModels.OPENAI_GPT_3_5}:
            return ModelClientFactory._create_openai_client(model)
        else:
            raise ValueError(f"Unsupported model: {model}")

    @staticmethod
    def _create_gemini_client(model: LLMModels):
        api_key = os.environ.get("GEMINI_API_KEY_FOR_DATA_AGENCY")

        if not api_key:
            raise RuntimeError(
                "GEMINI_API_KEY_FOR_DATA_AGENCY environment variable is not set. Set .env file following readme of data-agency."
            )

        return OpenAIChatCompletionClient(
            model=model.value,
            api_key=api_key,
            base_url=GOOGLE_OPENAI_BASE_URL,
            temperature=0.0,
            max_tokens=100000,
            model_info=ModelInfo(
                vision=False, function_calling=True, json_output=True, family="unknown", structured_output=True
            ),
        )

    @staticmethod
    def _create_openai_client(model: LLMModels):
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY environment variable is not set. Set .env file following readme of data-agency."
            )

        return OpenAIChatCompletionClient(
            model=model.value,
            api_key=api_key,
            temperature=0.0,
            max_tokens=100000,
            model_info=ModelInfo(
                vision=False, function_calling=True, json_output=True, family="openai", structured_output=True
            ),
        )


class UsageTracker:
    def __init__(self, cache_store):
        self.cache_store = cache_store
        self._calls_key = "llm_total_calls"
        self._tokens_key = "llm_total_tokens"

    def get_usage(self, key: str) -> int:
        return self.cache_store.cache.get(key, 0)

    def increment_usage(self, key: str, amount: int = 1) -> int:
        current = self.get_usage(key)
        new_total = current + amount
        self.cache_store.cache.set(key, new_total)
        return new_total

    def check_limits(self):
        if self.get_usage(self._calls_key) >= MAX_TOTAL_CALLS:
            raise UsageLimitExceededError(f"Max calls exceeded: {MAX_TOTAL_CALLS}")
        if self.get_usage(self._tokens_key) >= MAX_TOTAL_TOKENS:
            raise UsageLimitExceededError(f"Max tokens exceeded: {MAX_TOTAL_TOKENS}")

    def update_usage_from_result(self, result: CreateResult):
        """Update usage tracking based on LLM result (only for non-cached results)"""
        if not result.cached:
            # Price for "gemini-2.5-flash-lite-preview-06-17"
            # prompt_tokens: USD 0.1 per 1M tokens.
            # Completion tokens: USD 0.4 per 1M tokens
            total_token = result.usage.prompt_tokens + result.usage.completion_tokens * 4
            self.increment_usage(self._calls_key, 1)
            self.increment_usage(self._tokens_key, total_token)
            return total_token
        return 0

    def get_stats(self) -> dict:
        return {
            "from": self.cache_store.cache.get("llm_usage_from", "unknown"),
            "calls": self.cache_store.cache.get("llm_total_calls", 0),
            "tokens": self.cache_store.cache.get("llm_total_tokens", 0),
            "cost_in_usd": self.cache_store.cache.get("llm_total_tokens", 0) / 1_000_000 * 0.1,
            "max_calls": MAX_TOTAL_CALLS,
            "max_tokens": MAX_TOTAL_TOKENS,
        }

    def reset_usage(self):
        self.cache_store.cache.set("llm_usage_from", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.cache_store.cache.set("llm_total_calls", 0)
        self.cache_store.cache.set("llm_total_tokens", 0)


# global cache store for chat completions
cache_store = DiskCacheStore[CHAT_CACHE_VALUE_TYPE](Cache(CACHE_PATH))


class FullLogChatClientCache(ChatCompletionCache):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.usage_tracker = UsageTracker(cache_store)

    async def create(self, messages: Sequence[LLMMessage], *args, **kwargs) -> CreateResult:
        # Check usage limits
        self.usage_tracker.check_limits()

        # Setup logging
        logger = get_logger()
        logger.info(f"========{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}========")
        req = []
        for msg in messages:
            if hasattr(msg, "content") and hasattr(msg, "source"):
                req.append(f"{msg.source}: {msg.content}")  # type: ignore
            else:
                req.append(f"{msg.content}")

        # Log request if in DEBUG mode
        if DEBUG:
            logger.info("*********************Request*********************")
            for msg in messages:
                logger.info(f"{msg=}")
            logger.info("**************************************************")

        # CORE LOGIC: Make the actual LLM call
        try:
            result = await super().create(messages, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error during LLM request: {e}")
            if not DEBUG:
                logger.info("*********************Full Request********************* \n" + "\n".join(req))
            raise e from None

        # Update usage tracking
        total_tokens = self.usage_tracker.update_usage_from_result(result)
        if total_tokens > 0:
            logger.info(f"Usage: {total_tokens}/{MAX_TOTAL_TOKENS} tokens")

        # Log the request and response
        logger.info("Request: \n" + req[-1])
        logger.info("------")
        logger.info("Response: ")
        logger.info(result.content)

        return result

    def get_usage_stats(self) -> dict:
        return self.usage_tracker.get_stats()

    def reset_usage(self):
        self.usage_tracker.reset_usage()


def create_client(*args, model: LLMModels = LLMModels.GEMINI25_FLASH, **kwargs) -> FullLogChatClientCache:
    """Returns a FullLogChatClientCache instance with the configured model client and cache store."""
    model_client = ModelClientFactory.create_client(model)
    return FullLogChatClientCache(*args, client=model_client, store=cache_store, **kwargs)


async def sample():
    cache_client = create_client()
    msg = "Hello. How are you doing today?"

    response = await cache_client.create([UserMessage(content=msg, source="user")])
    response = await cache_client.create([UserMessage(content=msg, source="user")])
    print(response)
    print("done")


def main():
    import asyncio
    import nest_asyncio

    nest_asyncio.apply()
    asyncio.run(sample())


if __name__ == "__main__":
    main()
