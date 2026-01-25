"""Torn API client for making requests to the Torn API."""

import aiohttp
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict


class TornAPIError(Exception):
    """Base exception for Torn API errors."""
    pass


class TornAPIClient:
    """Client for interacting with the Torn API."""

    BASE_URL = "https://api.torn.com"
    BASE_URL_V2 = "https://api.torn.com/v2"
    
    # Error code messages from Torn API documentation
    ERROR_MESSAGES = {
        0: "Unknown error",
        1: "Key is empty",
        2: "Incorrect Key",
        3: "Wrong type",
        4: "Wrong fields",
        5: "Too many requests (max 100 per minute)",
        6: "Incorrect ID",
        7: "Incorrect ID-entity relation (private data)",
        8: "IP block (temporary ban due to abuse)",
        9: "API disabled",
        10: "Key owner is in federal jail",
        11: "Key change error (can only change once per 60 seconds)",
        12: "Key read error",
        13: "Key temporarily disabled due to owner inactivity (7+ days offline)",
        14: "Daily read limit reached",
        15: "Temporary error (testing)",
        16: "Access level of this key is not high enough",
        17: "Backend error occurred, please try again",
        18: "API key has been paused by the owner",
        19: "Must be migrated to crimes 2.0",
        20: "Race not yet finished",
        21: "Incorrect category",
        22: "This selection is only available in API v1",
        23: "This selection is only available in API v2",
        24: "Closed temporarily"
    }
    
    def __init__(self):
        """Initialize the Torn API client."""
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limits: Dict[str, list] = defaultdict(list)  # Track request timestamps per key
        self.max_requests_per_minute = 100
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _check_rate_limit(self, key: str) -> bool:
        """Check if we can make a request without hitting rate limit."""
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)
        
        # Clean old timestamps
        self.rate_limits[key] = [
            ts for ts in self.rate_limits[key] 
            if ts > minute_ago
        ]
        
        return len(self.rate_limits[key]) < self.max_requests_per_minute
    
    def _record_request(self, key: str):
        """Record a request timestamp for rate limiting."""
        self.rate_limits[key].append(datetime.utcnow())
    
    async def _make_request(
        self,
        endpoint: str,
        key: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        use_v2: bool = False
    ) -> Dict[str, Any]:
        """Make a request to the Torn API. use_v2: use API v2 base URL."""
        if not self._check_rate_limit(key):
            raise TornAPIError(
                f"Rate limit exceeded. Maximum {self.max_requests_per_minute} requests per minute."
            )
        
        session = await self._get_session()
        base = self.BASE_URL_V2 if use_v2 else self.BASE_URL
        url = f"{base}/{endpoint}"
        
        # Add key to params
        request_params = params or {}
        request_params['key'] = key
        
        try:
            async with session.get(url, params=request_params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                self._record_request(key)
                
                if response.status != 200:
                    raise TornAPIError(f"HTTP {response.status}: {await response.text()}")
                
                data = await response.json()
                
                # Check for Torn API error codes
                if isinstance(data, dict) and 'error' in data:
                    error_code = data['error'].get('code', 0)
                    error_message = self.ERROR_MESSAGES.get(
                        error_code,
                        f"Unknown error code: {error_code}"
                    )
                    raise TornAPIError(f"Torn API Error {error_code}: {error_message}")
                
                return data
                
        except aiohttp.ClientError as e:
            raise TornAPIError(f"Network error: {str(e)}")
        except asyncio.TimeoutError:
            raise TornAPIError("Request timeout")
    
    async def get_user(
        self,
        key: str,
        user_id: Optional[int] = None,
        selections: Optional[list] = None
    ) -> Dict[str, Any]:
        """Get user information."""
        endpoint = "user"
        if user_id:
            endpoint = f"user/{user_id}"
        
        params = {}
        if selections:
            params['selections'] = ','.join(selections)
        
        return await self._make_request(endpoint, key, params)
    
    async def get_faction(
        self,
        key: str,
        faction_id: Optional[int] = None,
        selections: Optional[list] = None,
        stat: Optional[str] = None,
        use_v2: bool = False
    ) -> Dict[str, Any]:
        """Get faction information.
        
        Args:
            key: API key
            faction_id: Optional faction ID (uses key owner's faction if not provided)
            selections: List of selections (e.g., ['basic', 'contributors'])
            stat: Optional stat name for contributors selection (e.g., 'gymstrength')
            use_v2: Use API v2 endpoint (default: False)
        """
        endpoint = "faction"
        if faction_id:
            endpoint = f"faction/{faction_id}"
        
        params = {}
        if selections:
            params['selections'] = ','.join(selections)
        
        # Add stat parameter if provided (required for contributors selection)
        if stat:
            params['stat'] = stat
        
        return await self._make_request(endpoint, key, params, use_v2=use_v2)
    
    async def get_key_info(self, key: str) -> Dict[str, Any]:
        """Get information about an API key."""
        return await self._make_request("key", key, {'selections': 'info'})
    
    async def get_company(
        self,
        key: str,
        company_id: Optional[int] = None,
        selections: Optional[list] = None
    ) -> Dict[str, Any]:
        """Get company information."""
        endpoint = "company"
        if company_id:
            endpoint = f"company/{company_id}"
        
        params = {}
        if selections:
            params['selections'] = ','.join(selections)
        
        return await self._make_request(endpoint, key, params)
    
    async def get_property(
        self,
        key: str,
        property_id: int,
        selections: Optional[list] = None
    ) -> Dict[str, Any]:
        """Get property information."""
        endpoint = f"property/{property_id}"
        
        params = {}
        if selections:
            params['selections'] = ','.join(selections)
        
        return await self._make_request(endpoint, key, params)
    
    async def get_market(
        self,
        key: str,
        item_id: int,
        selections: Optional[list] = None
    ) -> Dict[str, Any]:
        """Get market information."""
        endpoint = f"market/{item_id}"
        
        params = {}
        if selections:
            params['selections'] = ','.join(selections)
        
        return await self._make_request(endpoint, key, params)
    
    async def get_torn(
        self,
        key: str,
        selections: Optional[list] = None
    ) -> Dict[str, Any]:
        """Get Torn-wide information."""
        params = {}
        if selections:
            params['selections'] = ','.join(selections)
        
        return await self._make_request("torn", key, params)
    
    async def get_organized_crimes(self, key: str) -> Dict[str, Any]:
        """Get list of available organized crimes (public endpoint, v2).
        
        Returns general information about available organized crimes.
        """
        return await self._make_request("torn", key, {'selections': 'organizedcrimes'}, use_v2=True)
    
    async def get_faction_crimes(
        self,
        key: str,
        offset: Optional[int] = None,
        sort: Optional[str] = None,
        cat: Optional[str] = None,
        from_timestamp: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get faction crime data (requires faction permission, v2).
        
        Uses the key owner's faction automatically.
        
        Args:
            key: API key with faction permission (must be from a member of the faction)
            offset: Optional offset for pagination (default: 0)
            sort: Optional sort order - "ASC" or "DESC" (default: "DESC")
            cat: Optional category filter - filter by status to include specific status only
            from_timestamp: Optional unix timestamp to limit results to crimes created after this time
        
        Returns current and historical crime data for the key owner's faction.
        """
        params = {}
        if offset is not None:
            params['offset'] = offset
        if sort:
            params['sort'] = sort
        if cat:
            params['cat'] = cat
        if from_timestamp is not None:
            params['from'] = from_timestamp
        
        # Endpoint is /v2/faction/crimes (not /v2/faction with selections=crime)
        return await self._make_request("faction/crimes", key, params, use_v2=True)
    
    async def get_item(self, key: str, item_id: int) -> Dict[str, Any]:
        """Get item information from Torn API (v2).
        
        Args:
            key: API key
            item_id: Item ID to look up
        
        Returns item details including name, description, type, market_value, etc.
        """
        return await self._make_request(f"torn/{item_id}/items", key, {}, use_v2=True)
    
    async def get_user_discord(self, key: str, user_id: int) -> Dict[str, Any]:
        """Get user Discord information from Torn API (v2).
        
        Args:
            key: API key
            user_id: User ID to look up
        
        Returns user data including discord object with discord_id if linked.
        """
        return await self._make_request(f"user/{user_id}/discord", key, {}, use_v2=True)
