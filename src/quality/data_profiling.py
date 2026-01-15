"""Data profiling, quality reporting, and anomaly detection."""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
import numpy as np

from utils import get_logger
from .validators import ValidationResult

logger = get_logger(__name__)


@dataclass
class ColumnProfile:
    """Profile statistics for a single column."""
    column_name: str
    data_type: str
    total_count: int
    null_count: int
    null_percentage: float
    unique_count: int
    unique_percentage: float
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_value: Optional[float] = None
    quartiles: Optional[Dict[str, float]] = None
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class QualityReport:
    """Comprehensive data quality report."""
    timestamp: datetime
    total_rows: int
    total_columns: int
    column_profiles: List[ColumnProfile]
    validation_results: Optional[ValidationResult] = None
    quality_score: float = 0.0
    issues: List[Dict[str, Any]] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    timestamp: datetime
    total_anomalies: int
    anomalies: List[Dict[str, Any]]
    anomaly_score: float
    threshold: float
    method: str


def profile_data(df: pd.DataFrame, sample_size: int = 5) -> List[ColumnProfile]:
    """
    Generate profile statistics for each column in DataFrame.
    
    Args:
        df: DataFrame to profile
        sample_size: Number of sample values to include per column
    
    Returns:
        List of ColumnProfile objects
    """
    if df.empty:
        logger.warning("DataFrame is empty, returning empty profile")
        return []
    
    profiles = []
    
    for col in df.columns:
        col_data = df[col]
        total_count = len(col_data)
        null_count = col_data.isna().sum()
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0.0
        unique_count = col_data.nunique()
        unique_percentage = (unique_count / total_count * 100) if total_count > 0 else 0.0
        
        profile = ColumnProfile(
            column_name=col,
            data_type=str(col_data.dtype),
            total_count=total_count,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_count=unique_count,
            unique_percentage=unique_percentage
        )
        
        # Numeric statistics
        if pd.api.types.is_numeric_dtype(col_data):
            non_null_data = col_data.dropna()
            if len(non_null_data) > 0:
                profile.min_value = float(non_null_data.min())
                profile.max_value = float(non_null_data.max())
                profile.mean_value = float(non_null_data.mean())
                profile.median_value = float(non_null_data.median())
                profile.std_value = float(non_null_data.std()) if len(non_null_data) > 1 else 0.0
                
                # Quartiles
                quartiles = non_null_data.quantile([0.25, 0.5, 0.75])
                profile.quartiles = {
                    'q1': float(quartiles[0.25]),
                    'median': float(quartiles[0.5]),
                    'q3': float(quartiles[0.75])
                }
        
        # Sample values
        non_null_sample = col_data.dropna().head(sample_size)
        profile.sample_values = non_null_sample.tolist()[:sample_size]
        
        profiles.append(profile)
    
    return profiles


def generate_quality_report(
    df: pd.DataFrame,
    validation_result: Optional[ValidationResult] = None,
    include_profiling: bool = True
) -> QualityReport:
    """
    Generate comprehensive data quality report.
    
    Args:
        df: DataFrame to analyze
        validation_result: Optional validation result to include
        include_profiling: Whether to include column profiling
    
    Returns:
        QualityReport with all quality metrics
    """
    timestamp = datetime.now()
    total_rows = len(df)
    total_columns = len(df.columns) if not df.empty else 0
    
    # Generate column profiles
    column_profiles = []
    if include_profiling and not df.empty:
        column_profiles = profile_data(df)
    
    # Calculate quality score (0-100)
    quality_score = 100.0
    issues = []
    recommendations = []
    
    if df.empty:
        quality_score = 0.0
        issues.append({
            'severity': 'critical',
            'issue': 'DataFrame is empty',
            'recommendation': 'Check data source and ingestion process'
        })
        return QualityReport(
            timestamp=timestamp,
            total_rows=0,
            total_columns=0,
            column_profiles=[],
            validation_results=validation_result,
            quality_score=0.0,
            issues=issues,
            recommendations=recommendations
        )
    
    # Check for high null percentages
    for profile in column_profiles:
        if profile.null_percentage > 50:
            quality_score -= 10
            issues.append({
                'severity': 'high',
                'column': profile.column_name,
                'issue': f'High null percentage: {profile.null_percentage:.2f}%',
                'recommendation': f'Investigate why {profile.column_name} has so many nulls'
            })
            recommendations.append(f'Review data source for {profile.column_name}')
        elif profile.null_percentage > 20:
            quality_score -= 5
            issues.append({
                'severity': 'medium',
                'column': profile.column_name,
                'issue': f'Moderate null percentage: {profile.null_percentage:.2f}%',
                'recommendation': f'Consider data quality checks for {profile.column_name}'
            })
    
    # Check for low uniqueness (potential duplicates)
    for profile in column_profiles:
        if profile.unique_percentage < 10 and profile.total_count > 100:
            quality_score -= 5
            issues.append({
                'severity': 'medium',
                'column': profile.column_name,
                'issue': f'Low uniqueness: {profile.unique_percentage:.2f}%',
                'recommendation': f'Check for duplicates or data quality issues in {profile.column_name}'
            })
    
    # Include validation results if provided
    if validation_result:
        if not validation_result.is_valid:
            error_count = len(validation_result.errors)
            quality_score -= min(error_count * 2, 30)  # Max 30 points deduction
            issues.append({
                'severity': 'high',
                'issue': f'Validation failed with {error_count} errors',
                'error_count': error_count,
                'invalid_rows': validation_result.invalid_rows,
                'recommendation': 'Review validation errors and fix data quality issues'
            })
            recommendations.append('Address validation errors before processing')
        
        if validation_result.warnings:
            quality_score -= len(validation_result.warnings)
            issues.append({
                'severity': 'medium',
                'issue': f'Validation warnings: {len(validation_result.warnings)}',
                'warnings': validation_result.warnings,
                'recommendation': 'Review validation warnings'
            })
    
    # Ensure quality score is between 0 and 100
    quality_score = max(0.0, min(100.0, quality_score))
    
    # Add general recommendations
    if quality_score < 70:
        recommendations.append('Data quality is below acceptable threshold. Review and fix issues.')
    elif quality_score < 90:
        recommendations.append('Data quality is acceptable but could be improved.')
    
    return QualityReport(
        timestamp=timestamp,
        total_rows=total_rows,
        total_columns=total_columns,
        column_profiles=column_profiles,
        validation_results=validation_result,
        quality_score=quality_score,
        issues=issues,
        recommendations=recommendations
    )


def detect_anomalies(
    df: pd.DataFrame,
    method: str = 'iqr',
    threshold: float = 3.0,
    columns: Optional[List[str]] = None
) -> AnomalyResult:
    """
    Detect anomalies in DataFrame using statistical methods.
    
    Methods:
    - 'iqr': Interquartile Range method
    - 'zscore': Z-score method
    - 'isolation': Isolation Forest (requires scikit-learn)
    
    Args:
        df: DataFrame to analyze
        method: Anomaly detection method
        threshold: Threshold for anomaly detection (multiplier for IQR, z-score threshold)
        columns: List of columns to check (if None, checks all numeric columns)
    
    Returns:
        AnomalyResult with detected anomalies
    """
    timestamp = datetime.now()
    anomalies = []
    
    if df.empty:
        return AnomalyResult(
            timestamp=timestamp,
            total_anomalies=0,
            anomalies=[],
            anomaly_score=0.0,
            threshold=threshold,
            method=method
        )
    
    # Default to numeric columns
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    if not columns:
        logger.warning("No numeric columns found for anomaly detection")
        return AnomalyResult(
            timestamp=timestamp,
            total_anomalies=0,
            anomalies=[],
            anomaly_score=0.0,
            threshold=threshold,
            method=method
        )
    
    total_anomalies = 0
    
    for col in columns:
        if col not in df.columns:
            continue
        
        col_data = df[col].dropna()
        if len(col_data) < 3:  # Need at least 3 values for statistical methods
            continue
        
        if method == 'iqr':
            q1 = col_data.quantile(0.25)
            q3 = col_data.quantile(0.75)
            iqr = q3 - q1
            
            if iqr == 0:  # All values are the same
                continue
            
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr
            
            anomaly_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            anomaly_indices = df[anomaly_mask].index.tolist()
            
            for idx in anomaly_indices:
                value = df.loc[idx, col]
                anomalies.append({
                    'row': idx,
                    'column': col,
                    'value': float(value) if pd.notna(value) else None,
                    'method': 'iqr',
                    'lower_bound': float(lower_bound),
                    'upper_bound': float(upper_bound),
                    'reason': f'Value {value} outside IQR bounds [{lower_bound:.2f}, {upper_bound:.2f}]'
                })
                total_anomalies += 1
        
        elif method == 'zscore':
            mean = col_data.mean()
            std = col_data.std()
            
            if std == 0:  # All values are the same
                continue
            
            z_scores = np.abs((df[col] - mean) / std)
            anomaly_mask = (z_scores > threshold) & df[col].notna()
            anomaly_indices = df[anomaly_mask].index.tolist()
            
            for idx in anomaly_indices:
                value = df.loc[idx, col]
                z_score = float(z_scores.loc[idx])
                anomalies.append({
                    'row': idx,
                    'column': col,
                    'value': float(value) if pd.notna(value) else None,
                    'method': 'zscore',
                    'z_score': z_score,
                    'mean': float(mean),
                    'std': float(std),
                    'reason': f'Z-score {z_score:.2f} exceeds threshold {threshold}'
                })
                total_anomalies += 1
        
        else:
            logger.warning(f"Unknown anomaly detection method: {method}")
    
    # Calculate anomaly score (percentage of rows with anomalies)
    anomaly_score = (total_anomalies / len(df) * 100) if len(df) > 0 else 0.0
    
    if total_anomalies > 0:
        logger.warning(f"Detected {total_anomalies} anomalies using {method} method")
    
    return AnomalyResult(
        timestamp=timestamp,
        total_anomalies=total_anomalies,
        anomalies=anomalies,
        anomaly_score=anomaly_score,
        threshold=threshold,
        method=method
    )


def print_quality_report(report: QualityReport) -> None:
    """
    Print quality report in a readable format.
    
    Args:
        report: QualityReport to print
    """
    print("=" * 80)
    print("DATA QUALITY REPORT")
    print("=" * 80)
    print(f"Timestamp: {report.timestamp}")
    print(f"Total Rows: {report.total_rows:,}")
    print(f"Total Columns: {report.total_columns}")
    print(f"Quality Score: {report.quality_score:.2f}/100")
    print()
    
    if report.issues:
        print("ISSUES:")
        print("-" * 80)
        for i, issue in enumerate(report.issues, 1):
            severity = issue.get('severity', 'unknown').upper()
            print(f"{i}. [{severity}] {issue.get('issue', 'Unknown issue')}")
            if 'recommendation' in issue:
                print(f"   Recommendation: {issue['recommendation']}")
        print()
    
    if report.recommendations:
        print("RECOMMENDATIONS:")
        print("-" * 80)
        for i, rec in enumerate(report.recommendations, 1):
            print(f"{i}. {rec}")
        print()
    
    if report.column_profiles:
        print("COLUMN PROFILES:")
        print("-" * 80)
        for profile in report.column_profiles:
            print(f"\nColumn: {profile.column_name}")
            print(f"  Type: {profile.data_type}")
            print(f"  Total: {profile.total_count:,}")
            print(f"  Nulls: {profile.null_count:,} ({profile.null_percentage:.2f}%)")
            print(f"  Unique: {profile.unique_count:,} ({profile.unique_percentage:.2f}%)")
            
            if profile.min_value is not None:
                print(f"  Min: {profile.min_value}")
                print(f"  Max: {profile.max_value}")
                print(f"  Mean: {profile.mean_value:.4f}")
                print(f"  Median: {profile.median_value:.4f}")
                print(f"  Std: {profile.std_value:.4f}")
            
            if profile.sample_values:
                print(f"  Sample: {profile.sample_values}")
    
    print("=" * 80)
