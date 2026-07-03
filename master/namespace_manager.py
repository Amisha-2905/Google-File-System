import threading
from typing import Dict, List, Optional, Tuple

class NamespaceManager:
    def __init__(self):
        self._root: Dict = {}
        self._lock = threading.Lock()

    def _split_path(self, path: str) -> List[str]:
        """Helper to sanitize and split a path string into components."""
        return [c for c in path.strip("/").split("/") if c]

    def resolve_path(self, path: str) -> Tuple[bool, Optional[Dict]]:
        """
        Traverses the directory tree to find the node at the given path.
        Returns (exists, node_content).
        """
        components = self._split_path(path)
        if not components:
            return True, self._root

        with self._lock:
            current = self._root
            for part in components:
                if not isinstance(current, dict) or part not in current:
                    return False, None
                current = current[part]
            return True, current

    def create_entry(self, path: str, is_dir: bool = False) -> bool:
        """
        Creates a file or directory entry in the tree.
        Returns True if successful, False if the path already exists or parent missing.
        """
        components = self._split_path(path)
        if not components:
            return False  # Cannot recreate root

        filename = components[-1]
        parent_parts = components[:-1]

        with self._lock:
            # 1. Navigate to parent directory
            current = self._root
            for part in parent_parts:
                if not isinstance(current, dict) or part not in current:
                    return False  # Parent directory does not exist
                current = current[part]

            if not isinstance(current, dict):
                return False  # Parent component is a file, not a directory

            # 2. Check if file/dir already exists
            if filename in current:
                return False  # Already exists

            # 3. Insert the entry
            current[filename] = {} if is_dir else "FILE"
            return True

    def delete_entry(self, path: str) -> bool:
        """
        Removes a file or directory from the tree.
        Returns True if successfully deleted, False otherwise.
        """
        components = self._split_path(path)
        if not components:
            return False

        filename = components[-1]
        parent_parts = components[:-1]

        with self._lock:
            current = self._root
            for part in parent_parts:
                if not isinstance(current, dict) or part not in current:
                    return False
                current = current[part]

            if not isinstance(current, dict) or filename not in current:
                return False

            del current[filename]
            return True