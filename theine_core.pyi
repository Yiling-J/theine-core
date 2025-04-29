from typing import Optional, List, Tuple

class CoreDebugInfo:
    len: int
    window_len: int
    probation_len: int
    protected_len: int

class TlfuCore:
    """
    A Python class representing the TlfuCore Rust struct.

    Note:
        None of the methods in this class are thread-safe.
        Ensure that you use the appropriate mutex on the caller side.

    """

    def __init__(self, size: int) -> None:
        """
        Initializes a new TlfuCore instance with the given size.

        :param size: The maximum number of entries the cache can hold.
        """
        ...

    def set(self, entries: List[Tuple[int, int]]) -> List[int]:
        """
        Sets multiple entries in the cache.

        :param entries: A list of tuples where each tuple contains a key and its time-to-live (TTL) in nanoseconds.
        :return: A list of keys that were evicted.
        """
        ...

    def remove(self, key: int) -> Optional[int]:
        """
        Removes an entry from the cache by its key.

        :param key: The key of the entry to remove.
        :return: The removed key if it was present, otherwise None.
        """
        ...

    def access(self, keys: List[int]) -> None:
        """
        Marks multiple keys as accessed, updating their status in the cache.

        :param keys: A list of keys to mark as accessed.
        """
        ...

    def advance(self) -> List[int]:
        """
        Advances the internal clock and cleans up expired entries.
        """
        ...

    def clear(self) -> None:
        """
        Clears all entries from the cache.
        """
        ...

    def len(self) -> int:
        """
        Returns the number of entries currently in the cache.

        :return: The number of entries.
        """
        ...

    def debug_info(self) -> CoreDebugInfo:
        """
        Returns the debug info of core.

        :return: Debug info.
        """
        ...

    def keys(self) -> List[int]:
        """
        Returns all keys, used in test only.

        :return: Keys list.
        """
        ...

def spread(h: int) -> int:
    """
    Applies a supplemental hash function to a given hash value.

    Python's hash function returns an int, which could be negative.
    This function spreads the hash to make it more uniformly distributed.

    :param h: The original hash value (may be negative).
    :return: A uniformly distributed hash value as a positive integer.
    """
    ...

class BloomFilter:
    def put(self, key: str) -> None: ...
    def contains(self, key: str) -> bool: ...
