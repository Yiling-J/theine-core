from typing import Optional, Tuple, Any, Dict, List

class TlfuCore:
    def __init__(self, size: int): ...
    def set(self, key: str, ttl: int) -> Tuple[int, Optional[int], Optional[str]]: ...
    def remove(self, key: str) -> Optional[int]: ...
    def access(self, key: str) -> Optional[int]: ...
    def advance(self, cache: List, sentinel: Any, kh: Dict, hk: Dict): ...
    def clear(self): ...
    def len(self) -> int: ...

class ClockProCore:
    def __init__(self, size: int): ...
    def set(
        self, key: str, ttl: int
    ) -> Tuple[int, Optional[int], Optional[int], Optional[str]]: ...
    def remove(self, key: str) -> Optional[int]: ...
    def access(self, key: str) -> Optional[int]: ...
    def advance(self, cache: List, sentinel: Any, kh: Dict, hk: Dict): ...
    def clear(self): ...
    def len(self) -> int: ...

class LruCore:
    def __init__(self, size: int): ...
    def set(self, key: str, ttl: int) -> Tuple[int, Optional[int], Optional[str]]: ...
    def remove(self, key: str) -> Optional[int]: ...
    def access(self, key: str) -> Optional[int]: ...
    def advance(self, cache: List, sentinel: Any, kh: Dict, hk: Dict): ...
    def clear(self): ...
    def len(self) -> int: ...

class BloomFilter:
    def put(self, key: str): ...
    def contains(self, key: str) -> bool: ...
