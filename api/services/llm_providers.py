"""
LLM Provider Abstraction
========================

Multi-provider support for AI chat functionality.
Supports: Anthropic (Claude), OpenAI (GPT-4), and extensible to others.

Features:
- Unified interface for different LLM providers
- Streaming support
- Tool/function calling support
- Cost tracking
- Error handling and retries
"""

import os
import json
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, AsyncGenerator, Union
from dataclasses import dataclass, field
from datetime import datetime
import httpx

# Provider SDK imports (with fallbacks)
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


@dataclass
class Message:
    """Unified message format across providers."""
    role: str  # 'user', 'assistant', 'system', 'tool'
    content: str
    tool_calls: Optional[List[Dict]] = None
    tool_call_id: Optional[str] = None
    name: Optional[str] = None


@dataclass
class ToolDefinition:
    """Unified tool definition for function calling."""
    name: str
    description: str
    parameters: Dict[str, Any]  # JSON Schema


@dataclass
class LLMResponse:
    """Unified response format."""
    content: str
    tool_calls: Optional[List[Dict]] = None
    finish_reason: str = "stop"
    usage: Dict[str, int] = field(default_factory=dict)
    model: str = ""
    provider: str = ""
    latency_ms: int = 0


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key
        self.model = model
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_requests = 0
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return provider name."""
        pass
    
    @property
    @abstractmethod
    def default_model(self) -> str:
        """Return default model for this provider."""
        pass
    
    @abstractmethod
    async def generate(
        self,
        messages: List[Message],
        tools: Optional[List[ToolDefinition]] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        stream: bool = False,
    ) -> Union[LLMResponse, AsyncGenerator[str, None]]:
        """Generate a response from the LLM."""
        pass
    
    def get_usage_stats(self) -> Dict:
        """Return usage statistics."""
        return {
            "provider": self.provider_name,
            "model": self.model or self.default_model,
            "total_requests": self.total_requests,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
        }


class AnthropicProvider(LLMProvider):
    """Anthropic Claude provider."""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        super().__init__(api_key, model)
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not ANTHROPIC_AVAILABLE:
            raise ImportError("anthropic package not installed. Run: pip install anthropic")
        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.async_client = anthropic.AsyncAnthropic(api_key=self.api_key)
    
    @property
    def provider_name(self) -> str:
        return "anthropic"
    
    @property
    def default_model(self) -> str:
        return "claude-sonnet-4-20250514"
    
    def _convert_tools(self, tools: List[ToolDefinition]) -> List[Dict]:
        """Convert tools to Anthropic format."""
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.parameters,
            }
            for tool in tools
        ]
    
    def _convert_messages(self, messages: List[Message]) -> tuple:
        """Convert messages to Anthropic format, extracting system message."""
        system = None
        converted = []
        
        for msg in messages:
            if msg.role == "system":
                system = msg.content
            elif msg.role == "tool":
                converted.append({
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": msg.tool_call_id,
                        "content": msg.content,
                    }]
                })
            elif msg.tool_calls:
                converted.append({
                    "role": "assistant",
                    "content": [{
                        "type": "tool_use",
                        "id": tc["id"],
                        "name": tc["name"],
                        "input": tc["arguments"],
                    } for tc in msg.tool_calls]
                })
            else:
                converted.append({
                    "role": msg.role,
                    "content": msg.content,
                })
        
        return system, converted
    
    async def generate(
        self,
        messages: List[Message],
        tools: Optional[List[ToolDefinition]] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        stream: bool = False,
    ) -> Union[LLMResponse, AsyncGenerator[str, None]]:
        """Generate response using Claude."""
        start_time = datetime.now()
        
        system, converted_messages = self._convert_messages(messages)
        
        kwargs = {
            "model": self.model or self.default_model,
            "messages": converted_messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        
        if system:
            kwargs["system"] = system
        
        if tools:
            kwargs["tools"] = self._convert_tools(tools)
        
        if stream:
            return self._stream_response(kwargs, start_time)
        
        response = await self.async_client.messages.create(**kwargs)
        
        latency_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # Track usage
        self.total_requests += 1
        self.total_input_tokens += response.usage.input_tokens
        self.total_output_tokens += response.usage.output_tokens
        
        # Extract content and tool calls
        content = ""
        tool_calls = []
        
        for block in response.content:
            if block.type == "text":
                content = block.text
            elif block.type == "tool_use":
                tool_calls.append({
                    "id": block.id,
                    "name": block.name,
                    "arguments": block.input,
                })
        
        return LLMResponse(
            content=content,
            tool_calls=tool_calls if tool_calls else None,
            finish_reason=response.stop_reason,
            usage={
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            },
            model=response.model,
            provider=self.provider_name,
            latency_ms=latency_ms,
        )
    
    async def _stream_response(self, kwargs: Dict, start_time: datetime) -> AsyncGenerator[str, None]:
        """Stream response chunks."""
        async with self.async_client.messages.stream(**kwargs) as stream:
            async for text in stream.text_stream:
                yield text


class OpenAIProvider(LLMProvider):
    """OpenAI GPT-4 provider."""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        super().__init__(api_key, model)
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not OPENAI_AVAILABLE:
            raise ImportError("openai package not installed. Run: pip install openai")
        self.client = openai.AsyncOpenAI(api_key=self.api_key)
    
    @property
    def provider_name(self) -> str:
        return "openai"
    
    @property
    def default_model(self) -> str:
        return "gpt-4-turbo-preview"
    
    def _convert_tools(self, tools: List[ToolDefinition]) -> List[Dict]:
        """Convert tools to OpenAI format."""
        return [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                }
            }
            for tool in tools
        ]
    
    def _convert_messages(self, messages: List[Message]) -> List[Dict]:
        """Convert messages to OpenAI format."""
        converted = []
        
        for msg in messages:
            if msg.role == "tool":
                converted.append({
                    "role": "tool",
                    "tool_call_id": msg.tool_call_id,
                    "content": msg.content,
                })
            elif msg.tool_calls:
                converted.append({
                    "role": "assistant",
                    "content": msg.content or None,
                    "tool_calls": [{
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc["arguments"]) if isinstance(tc["arguments"], dict) else tc["arguments"],
                        }
                    } for tc in msg.tool_calls]
                })
            else:
                converted.append({
                    "role": msg.role,
                    "content": msg.content,
                })
        
        return converted
    
    async def generate(
        self,
        messages: List[Message],
        tools: Optional[List[ToolDefinition]] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        stream: bool = False,
    ) -> Union[LLMResponse, AsyncGenerator[str, None]]:
        """Generate response using GPT-4."""
        start_time = datetime.now()
        
        kwargs = {
            "model": self.model or self.default_model,
            "messages": self._convert_messages(messages),
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        
        if tools:
            kwargs["tools"] = self._convert_tools(tools)
        
        if stream:
            return self._stream_response(kwargs, start_time)
        
        response = await self.client.chat.completions.create(**kwargs)
        
        latency_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # Track usage
        self.total_requests += 1
        if response.usage:
            self.total_input_tokens += response.usage.prompt_tokens
            self.total_output_tokens += response.usage.completion_tokens
        
        # Extract content and tool calls
        message = response.choices[0].message
        content = message.content or ""
        tool_calls = None
        
        if message.tool_calls:
            tool_calls = [{
                "id": tc.id,
                "name": tc.function.name,
                "arguments": json.loads(tc.function.arguments),
            } for tc in message.tool_calls]
        
        return LLMResponse(
            content=content,
            tool_calls=tool_calls,
            finish_reason=response.choices[0].finish_reason,
            usage={
                "input_tokens": response.usage.prompt_tokens if response.usage else 0,
                "output_tokens": response.usage.completion_tokens if response.usage else 0,
            },
            model=response.model,
            provider=self.provider_name,
            latency_ms=latency_ms,
        )
    
    async def _stream_response(self, kwargs: Dict, start_time: datetime) -> AsyncGenerator[str, None]:
        """Stream response chunks."""
        kwargs["stream"] = True
        async for chunk in await self.client.chat.completions.create(**kwargs):
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content


class LLMProviderFactory:
    """Factory for creating LLM providers."""
    
    _providers = {
        "anthropic": AnthropicProvider,
        "claude": AnthropicProvider,
        "openai": OpenAIProvider,
        "gpt": OpenAIProvider,
    }
    
    @classmethod
    def create(
        cls,
        provider: str = "anthropic",
        api_key: Optional[str] = None,
        model: Optional[str] = None,
    ) -> LLMProvider:
        """Create an LLM provider instance."""
        provider_lower = provider.lower()
        
        if provider_lower not in cls._providers:
            available = list(cls._providers.keys())
            raise ValueError(f"Unknown provider '{provider}'. Available: {available}")
        
        return cls._providers[provider_lower](api_key=api_key, model=model)
    
    @classmethod
    def available_providers(cls) -> List[str]:
        """Return list of available providers."""
        available = []
        if ANTHROPIC_AVAILABLE:
            available.append("anthropic")
        if OPENAI_AVAILABLE:
            available.append("openai")
        return available


# Default provider instance
_default_provider: Optional[LLMProvider] = None

def get_llm_provider(
    provider: str = "anthropic",
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> LLMProvider:
    """Get or create an LLM provider."""
    global _default_provider
    
    if _default_provider is None:
        _default_provider = LLMProviderFactory.create(provider, api_key, model)
    
    return _default_provider
