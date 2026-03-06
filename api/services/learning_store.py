"""
Learning Store
==============

Stores and retrieves feedback, corrections, and examples for the AI system.
Enables the agent to learn and improve over time.

Features:
1. Store user feedback (thumbs up/down, corrections)
2. Store successful query examples
3. Retrieve similar past queries for context
4. Track correction patterns to improve future responses
5. Persist to S3 for durability
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import boto3
from botocore.exceptions import ClientError


@dataclass
class Feedback:
    """User feedback on a response."""
    feedback_id: str
    query_id: str
    user_id: str
    rating: str  # 'positive', 'negative', 'correction'
    original_question: str
    original_response: str
    correction: Optional[str] = None
    correct_answer: Optional[str] = None
    tags: List[str] = None
    created_at: str = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()


@dataclass
class LearnedExample:
    """A successful query that can be used as few-shot example."""
    example_id: str
    question: str
    answer: str
    sql_used: Optional[str] = None
    tools_used: List[str] = None
    domain: str = "general"  # enrollment, stars, risk_scores, policy
    rating_count: int = 0
    average_rating: float = 0.0
    created_at: str = None
    last_used: str = None
    
    def __post_init__(self):
        if self.tools_used is None:
            self.tools_used = []
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()


@dataclass
class CorrectionRule:
    """A learned rule from user corrections."""
    rule_id: str
    trigger_pattern: str  # What triggers this rule (regex or keyword)
    incorrect_behavior: str  # What the agent was doing wrong
    correct_behavior: str  # What the agent should do instead
    example_question: str
    example_correction: str
    confidence: float = 1.0
    times_applied: int = 0
    created_at: str = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()


class LearningStore:
    """
    Persistent storage for AI learning data.
    
    Stores:
    - User feedback (positive/negative/corrections)
    - Successful query examples (few-shot learning)
    - Correction rules (explicit rules derived from feedback)
    """
    
    def __init__(self, bucket: str = None, prefix: str = "learning"):
        self.bucket = bucket or os.environ.get("S3_BUCKET", "ma-data123")
        self.prefix = prefix
        self.s3 = boto3.client('s3')
        
        # In-memory caches
        self._feedback_cache: Dict[str, Feedback] = {}
        self._examples_cache: Dict[str, LearnedExample] = {}
        self._rules_cache: Dict[str, CorrectionRule] = {}
        
        # Load caches
        self._load_caches()
    
    def _s3_key(self, category: str, item_id: str) -> str:
        """Generate S3 key for an item."""
        return f"{self.prefix}/{category}/{item_id}.json"
    
    def _load_caches(self):
        """Load data from S3 into memory caches."""
        for category, cache, cls in [
            ("feedback", self._feedback_cache, Feedback),
            ("examples", self._examples_cache, LearnedExample),
            ("rules", self._rules_cache, CorrectionRule),
        ]:
            try:
                response = self.s3.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=f"{self.prefix}/{category}/"
                )
                for obj in response.get('Contents', []):
                    try:
                        data = self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])
                        item_data = json.loads(data['Body'].read().decode('utf-8'))
                        item = cls(**item_data)
                        cache[getattr(item, f'{category[:-1]}_id' if category != 'feedback' else 'feedback_id')] = item
                    except Exception as e:
                        print(f"Warning: Could not load {obj['Key']}: {e}")
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    print(f"Warning: Could not list {category}: {e}")
    
    def _save_to_s3(self, category: str, item_id: str, data: Dict):
        """Save item to S3."""
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self._s3_key(category, item_id),
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"Warning: Could not save to S3: {e}")
    
    # =========================================================================
    # FEEDBACK OPERATIONS
    # =========================================================================
    def add_feedback(
        self,
        query_id: str,
        user_id: str,
        rating: str,
        original_question: str,
        original_response: str,
        correction: Optional[str] = None,
        correct_answer: Optional[str] = None,
        tags: List[str] = None,
    ) -> Feedback:
        """
        Add user feedback on a response.
        
        Args:
            query_id: ID of the query being rated
            user_id: User providing feedback
            rating: 'positive', 'negative', or 'correction'
            original_question: The original question asked
            original_response: The response the agent gave
            correction: User's correction text (if rating='correction')
            correct_answer: What the correct answer should be
            tags: Optional tags for categorization
            
        Returns:
            Created Feedback object
        """
        feedback_id = hashlib.md5(
            f"{query_id}:{user_id}:{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]
        
        feedback = Feedback(
            feedback_id=feedback_id,
            query_id=query_id,
            user_id=user_id,
            rating=rating,
            original_question=original_question,
            original_response=original_response,
            correction=correction,
            correct_answer=correct_answer,
            tags=tags or [],
        )
        
        # Save to cache and S3
        self._feedback_cache[feedback_id] = feedback
        self._save_to_s3("feedback", feedback_id, asdict(feedback))
        
        # If it's a correction, try to create a rule
        if rating == 'correction' and correction:
            self._maybe_create_rule(feedback)
        
        return feedback
    
    def get_feedback_stats(self) -> Dict:
        """Get summary statistics on feedback."""
        total = len(self._feedback_cache)
        positive = sum(1 for f in self._feedback_cache.values() if f.rating == 'positive')
        negative = sum(1 for f in self._feedback_cache.values() if f.rating == 'negative')
        corrections = sum(1 for f in self._feedback_cache.values() if f.rating == 'correction')
        
        return {
            "total_feedback": total,
            "positive": positive,
            "negative": negative,
            "corrections": corrections,
            "satisfaction_rate": positive / total if total > 0 else 0,
        }
    
    def get_recent_feedback(self, limit: int = 20) -> List[Feedback]:
        """Get recent feedback items."""
        sorted_feedback = sorted(
            self._feedback_cache.values(),
            key=lambda f: f.created_at,
            reverse=True
        )
        return sorted_feedback[:limit]
    
    # =========================================================================
    # EXAMPLES OPERATIONS
    # =========================================================================
    def add_example(
        self,
        question: str,
        answer: str,
        sql_used: Optional[str] = None,
        tools_used: List[str] = None,
        domain: str = "general",
    ) -> LearnedExample:
        """
        Add a successful query as a learned example.
        
        Args:
            question: The question that was asked
            answer: The correct answer
            sql_used: SQL query used (if any)
            tools_used: Tools that were used
            domain: Domain category
            
        Returns:
            Created LearnedExample
        """
        example_id = hashlib.md5(question.lower().encode()).hexdigest()[:16]
        
        # Check if similar example exists
        if example_id in self._examples_cache:
            existing = self._examples_cache[example_id]
            existing.rating_count += 1
            existing.average_rating = (existing.average_rating * (existing.rating_count - 1) + 1) / existing.rating_count
            existing.last_used = datetime.utcnow().isoformat()
            self._save_to_s3("examples", example_id, asdict(existing))
            return existing
        
        example = LearnedExample(
            example_id=example_id,
            question=question,
            answer=answer,
            sql_used=sql_used,
            tools_used=tools_used or [],
            domain=domain,
            rating_count=1,
            average_rating=1.0,
            last_used=datetime.utcnow().isoformat(),
        )
        
        self._examples_cache[example_id] = example
        self._save_to_s3("examples", example_id, asdict(example))
        
        return example
    
    def find_similar_examples(
        self,
        question: str,
        domain: Optional[str] = None,
        limit: int = 5,
    ) -> List[LearnedExample]:
        """
        Find examples similar to the given question.
        
        Uses simple keyword matching. For production, would use embeddings.
        
        Args:
            question: Question to find similar examples for
            domain: Optional domain filter
            limit: Max examples to return
            
        Returns:
            List of similar examples
        """
        question_lower = question.lower()
        keywords = set(question_lower.split())
        
        scored_examples = []
        for example in self._examples_cache.values():
            if domain and example.domain != domain:
                continue
            
            example_keywords = set(example.question.lower().split())
            overlap = len(keywords & example_keywords)
            
            if overlap > 0:
                score = overlap / max(len(keywords), len(example_keywords))
                # Boost by rating
                score *= (0.5 + 0.5 * example.average_rating)
                scored_examples.append((score, example))
        
        scored_examples.sort(key=lambda x: x[0], reverse=True)
        return [ex for _, ex in scored_examples[:limit]]
    
    def get_best_examples(self, domain: Optional[str] = None, limit: int = 10) -> List[LearnedExample]:
        """Get highest-rated examples for few-shot prompting."""
        filtered = self._examples_cache.values()
        if domain:
            filtered = [ex for ex in filtered if ex.domain == domain]
        
        sorted_examples = sorted(
            filtered,
            key=lambda ex: (ex.average_rating, ex.rating_count),
            reverse=True
        )
        return list(sorted_examples)[:limit]
    
    # =========================================================================
    # RULES OPERATIONS
    # =========================================================================
    def _maybe_create_rule(self, feedback: Feedback):
        """
        Try to create a correction rule from feedback.
        
        Only creates rules when we have enough signal.
        """
        # Look for similar corrections
        similar_corrections = [
            f for f in self._feedback_cache.values()
            if f.rating == 'correction'
            and f.feedback_id != feedback.feedback_id
            and self._questions_similar(f.original_question, feedback.original_question)
        ]
        
        # Need at least 2 similar corrections to create a rule
        if len(similar_corrections) < 1:
            return
        
        # Extract common pattern
        trigger_pattern = self._extract_trigger_pattern(
            [feedback.original_question] + [f.original_question for f in similar_corrections]
        )
        
        if not trigger_pattern:
            return
        
        rule_id = hashlib.md5(trigger_pattern.encode()).hexdigest()[:16]
        
        rule = CorrectionRule(
            rule_id=rule_id,
            trigger_pattern=trigger_pattern,
            incorrect_behavior=feedback.original_response[:500],
            correct_behavior=feedback.correction or feedback.correct_answer or "",
            example_question=feedback.original_question,
            example_correction=feedback.correction or feedback.correct_answer or "",
            confidence=0.7 + (0.1 * len(similar_corrections)),  # Higher confidence with more similar corrections
        )
        
        self._rules_cache[rule_id] = rule
        self._save_to_s3("rules", rule_id, asdict(rule))
    
    def _questions_similar(self, q1: str, q2: str) -> bool:
        """Check if two questions are similar (simple keyword overlap)."""
        words1 = set(q1.lower().split())
        words2 = set(q2.lower().split())
        overlap = len(words1 & words2)
        return overlap / max(len(words1), len(words2)) > 0.5
    
    def _extract_trigger_pattern(self, questions: List[str]) -> Optional[str]:
        """Extract common pattern from questions."""
        # Simple: find common words
        word_sets = [set(q.lower().split()) for q in questions]
        common = word_sets[0]
        for ws in word_sets[1:]:
            common &= ws
        
        if len(common) < 2:
            return None
        
        return " ".join(sorted(common)[:5])
    
    def get_applicable_rules(self, question: str) -> List[CorrectionRule]:
        """
        Get correction rules that apply to a question.
        
        Args:
            question: Question to check
            
        Returns:
            List of applicable rules
        """
        question_lower = question.lower()
        applicable = []
        
        for rule in self._rules_cache.values():
            pattern_words = set(rule.trigger_pattern.split())
            question_words = set(question_lower.split())
            
            if pattern_words.issubset(question_words):
                applicable.append(rule)
        
        return sorted(applicable, key=lambda r: r.confidence, reverse=True)
    
    def add_rule(
        self,
        trigger_pattern: str,
        incorrect_behavior: str,
        correct_behavior: str,
        example_question: str = "",
        example_correction: str = "",
    ) -> CorrectionRule:
        """
        Manually add a correction rule.
        
        Args:
            trigger_pattern: Keywords that trigger this rule
            incorrect_behavior: What the agent does wrong
            correct_behavior: What the agent should do
            example_question: Example question for context
            example_correction: Example of correct response
            
        Returns:
            Created CorrectionRule
        """
        rule_id = hashlib.md5(trigger_pattern.encode()).hexdigest()[:16]
        
        rule = CorrectionRule(
            rule_id=rule_id,
            trigger_pattern=trigger_pattern,
            incorrect_behavior=incorrect_behavior,
            correct_behavior=correct_behavior,
            example_question=example_question,
            example_correction=example_correction,
            confidence=1.0,  # Manual rules have high confidence
        )
        
        self._rules_cache[rule_id] = rule
        self._save_to_s3("rules", rule_id, asdict(rule))
        
        return rule
    
    def get_all_rules(self) -> List[CorrectionRule]:
        """Get all correction rules."""
        return list(self._rules_cache.values())
    
    # =========================================================================
    # CONTEXT BUILDING
    # =========================================================================
    def get_learning_context(self, question: str, domain: Optional[str] = None) -> Dict:
        """
        Get all relevant learning context for a question.
        
        Combines:
        - Similar examples (few-shot)
        - Applicable rules (corrections)
        
        Args:
            question: Current question
            domain: Optional domain filter
            
        Returns:
            Dict with examples and rules
        """
        return {
            "similar_examples": self.find_similar_examples(question, domain, limit=3),
            "applicable_rules": self.get_applicable_rules(question),
            "best_examples": self.get_best_examples(domain, limit=2),
        }


# Singleton instance
_learning_store: Optional[LearningStore] = None

def get_learning_store() -> LearningStore:
    """Get or create singleton learning store."""
    global _learning_store
    if _learning_store is None:
        _learning_store = LearningStore()
    return _learning_store
