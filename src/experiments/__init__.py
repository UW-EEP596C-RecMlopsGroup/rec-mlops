"""
Real-time streaming components
Kafka-based event processing and feature engineering
"""

from .feature_processor import FeatureProcessor
from .kafka_producer import KafkaProducer

__all__ = ["KafkaProducer", "FeatureProcessor"]
