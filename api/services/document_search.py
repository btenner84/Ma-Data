"""
CMS Document Search Service
============================

Provides search capability over scraped CMS documents.
Used by the agent to answer policy and methodology questions.

Features:
1. Full-text search across document corpus
2. Semantic search with embeddings (optional)
3. Document type and year filtering
4. Key changes extraction
"""

import os
import json
import re
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import boto3
from botocore.exceptions import ClientError


@dataclass
class SearchResult:
    """A search result from document search."""
    doc_id: str
    doc_type: str
    year: int
    title: str
    url: str
    score: float
    snippet: str
    key_changes: List[str]
    metadata: Dict


class DocumentSearchService:
    """
    Search service for CMS documents.
    
    Supports:
    - Keyword search across document text
    - Document filtering by type and year
    - Key changes lookup
    """
    
    def __init__(self, bucket: str = None, prefix: str = "documents"):
        self.bucket = bucket or os.environ.get("S3_BUCKET", "ma-data123")
        self.prefix = prefix
        self.s3 = boto3.client('s3')
        
        # In-memory index
        self._index: Dict[str, Dict] = {}
        self._texts: Dict[str, str] = {}
        
        # Load index
        self._load_index()
    
    def _load_index(self):
        """Load document index from S3."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/index.json"
            )
            index_data = json.loads(response['Body'].read().decode('utf-8'))
            
            for doc in index_data.get('documents', []):
                self._index[doc['doc_id']] = doc
            
            print(f"Loaded {len(self._index)} documents into search index")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("No document index found - run scrape_cms_documents.py first")
            else:
                print(f"Error loading index: {e}")
    
    def _load_document_text(self, doc_type: str, year: int) -> Optional[str]:
        """Load full text of a document."""
        key = f"{doc_type}:{year}"
        
        if key in self._texts:
            return self._texts[key]
        
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/text/{doc_type}/{year}.txt"
            )
            text = response['Body'].read().decode('utf-8')
            self._texts[key] = text
            return text
        except Exception as e:
            return None
    
    def _load_document_metadata(self, doc_type: str, year: int) -> Optional[Dict]:
        """Load document metadata."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/metadata/{doc_type}/{year}.json"
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception:
            return None
    
    def search(
        self,
        query: str,
        doc_types: List[str] = None,
        years: List[int] = None,
        limit: int = 10,
    ) -> List[SearchResult]:
        """
        Search documents for a query.
        
        Args:
            query: Search query (keywords)
            doc_types: Filter by document types
            years: Filter by years
            limit: Max results
            
        Returns:
            List of SearchResult objects
        """
        query_lower = query.lower()
        query_words = set(query_lower.split())
        
        results = []
        
        for doc_id, doc_info in self._index.items():
            # Apply filters
            if doc_types and doc_info['doc_type'] not in doc_types:
                continue
            if years and doc_info['year'] not in years:
                continue
            
            # Load text
            text = self._load_document_text(doc_info['doc_type'], doc_info['year'])
            
            if not text:
                continue
            
            text_lower = text.lower()
            
            # Calculate relevance score (simple keyword matching)
            word_matches = sum(1 for w in query_words if w in text_lower)
            if word_matches == 0:
                continue
            
            score = word_matches / len(query_words)
            
            # Boost score based on title match
            if query_lower in doc_info['title'].lower():
                score += 0.5
            
            # Find best snippet
            snippet = self._find_best_snippet(text, query_words)
            
            # Load metadata for key changes
            metadata = self._load_document_metadata(doc_info['doc_type'], doc_info['year'])
            key_changes = metadata.get('key_changes', []) if metadata else []
            
            results.append(SearchResult(
                doc_id=doc_id,
                doc_type=doc_info['doc_type'],
                year=doc_info['year'],
                title=doc_info['title'],
                url=doc_info['url'],
                score=score,
                snippet=snippet,
                key_changes=key_changes,
                metadata=metadata or {},
            ))
        
        # Sort by score and return top results
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:limit]
    
    def _find_best_snippet(self, text: str, query_words: set, context_chars: int = 300) -> str:
        """Find the best snippet containing query words."""
        text_lower = text.lower()
        
        # Find position of first query word
        best_pos = -1
        for word in query_words:
            pos = text_lower.find(word)
            if pos != -1 and (best_pos == -1 or pos < best_pos):
                best_pos = pos
        
        if best_pos == -1:
            return text[:context_chars] + "..."
        
        # Extract context around the match
        start = max(0, best_pos - context_chars // 2)
        end = min(len(text), best_pos + context_chars // 2)
        
        snippet = text[start:end]
        
        # Clean up
        if start > 0:
            snippet = "..." + snippet
        if end < len(text):
            snippet = snippet + "..."
        
        return snippet.strip()
    
    def get_document(self, doc_type: str, year: int) -> Optional[Dict]:
        """Get a specific document's full details."""
        metadata = self._load_document_metadata(doc_type, year)
        text = self._load_document_text(doc_type, year)
        
        if metadata:
            metadata['full_text'] = text
            return metadata
        
        return None
    
    def get_key_changes(
        self,
        doc_type: str = None,
        year: int = None,
    ) -> List[Dict]:
        """
        Get key changes from documents.
        
        Args:
            doc_type: Filter by document type
            year: Filter by year
            
        Returns:
            List of changes with document info
        """
        changes = []
        
        for doc_id, doc_info in self._index.items():
            if doc_type and doc_info['doc_type'] != doc_type:
                continue
            if year and doc_info['year'] != year:
                continue
            
            metadata = self._load_document_metadata(doc_info['doc_type'], doc_info['year'])
            
            if metadata and metadata.get('key_changes'):
                for change in metadata['key_changes']:
                    changes.append({
                        'doc_type': doc_info['doc_type'],
                        'year': doc_info['year'],
                        'title': doc_info['title'],
                        'change': change,
                    })
        
        return changes
    
    def get_policy_timeline(self, topic: str = None) -> List[Dict]:
        """
        Get a timeline of policy changes.
        
        Args:
            topic: Optional topic to filter by (e.g., "risk adjustment", "star ratings")
            
        Returns:
            Chronological list of changes
        """
        all_changes = self.get_key_changes()
        
        if topic:
            topic_lower = topic.lower()
            all_changes = [
                c for c in all_changes
                if topic_lower in c['change'].lower()
            ]
        
        # Sort by year
        all_changes.sort(key=lambda x: x['year'])
        
        return all_changes
    
    def list_available_documents(self) -> List[Dict]:
        """List all available documents."""
        return [
            {
                'doc_id': info['doc_id'],
                'doc_type': info['doc_type'],
                'year': info['year'],
                'title': info['title'],
                'url': info['url'],
                'has_text': info.get('has_text', False),
            }
            for info in self._index.values()
        ]


# Singleton instance
_service_instance: Optional[DocumentSearchService] = None

def get_document_search_service() -> DocumentSearchService:
    """Get or create singleton document search service."""
    global _service_instance
    if _service_instance is None:
        _service_instance = DocumentSearchService()
    return _service_instance
