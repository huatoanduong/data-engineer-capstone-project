"""Data quality validators using Pydantic models and custom validation functions."""

from pydantic import BaseModel, Field, model_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import pandas as pd
import numpy as np
from dataclasses import dataclass

from utils import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    total_rows: int
    valid_rows: int
    invalid_rows: int


class MarketDataSchema(BaseModel):
    """Pydantic model for market data validation."""
    timestamp: datetime
    symbol: str = Field(min_length=1)
    open: float = Field(gt=0, description="Opening price must be positive")
    high: float = Field(gt=0, description="High price must be positive")
    low: float = Field(gt=0, description="Low price must be positive")
    close: float = Field(gt=0, description="Closing price must be positive")
    volume: int = Field(ge=0, description="Volume must be non-negative")
    market_cap: Optional[int] = Field(ge=0, default=None, description="Market cap must be non-negative if provided")
    rsi: Optional[float] = Field(ge=0, le=100, default=None, description="RSI must be between 0 and 100 if provided")
    change_pct: Optional[float] = Field(default=None, description="Change percentage")
    asset_type: Optional[str] = Field(default=None, description="Asset type (stocks/crypto)")
    ingestion_timestamp: Optional[datetime] = Field(default=None, description="Ingestion timestamp")
    pe_ratio: Optional[float] = Field(gt=0, default=None, description="PE ratio must be positive if provided")
    
    @model_validator(mode='after')
    def validate_ohlc_relationships(self):
        """Validate OHLC relationships."""
        # Validate high >= open
        if self.high < self.open:
            raise ValueError('high must be >= open')
        
        # Validate low <= open
        if self.low > self.open:
            raise ValueError('low must be <= open')
        
        # Validate high >= low
        if self.high < self.low:
            raise ValueError('high must be >= low')
        
        # Validate open is between low and high
        if self.open < self.low or self.open > self.high:
            raise ValueError('open must be between low and high')
        
        # Validate close is between low and high
        if self.close < self.low or self.close > self.high:
            raise ValueError('close must be between low and high')
        
        return self


def validate_data(
    df: pd.DataFrame,
    schema_class: type[BaseModel] = MarketDataSchema,
    strict: bool = True
) -> ValidationResult:
    """
    Validate DataFrame against Pydantic schema.
    
    Args:
        df: DataFrame to validate
        schema_class: Pydantic model class to validate against
        strict: If True, stop on first error per row; if False, collect all errors
    
    Returns:
        ValidationResult with validation status and errors
    """
    errors = []
    warnings = []
    total_rows = len(df)
    valid_rows = 0
    invalid_rows = 0
    
    if df.empty:
        logger.warning("DataFrame is empty")
        return ValidationResult(
            is_valid=False,
            errors=[{'row': None, 'error': 'DataFrame is empty'}],
            warnings=[],
            total_rows=0,
            valid_rows=0,
            invalid_rows=0
        )
    
    for idx, row in df.iterrows():
        row_errors = []
        try:
            # Convert row to dict, handling NaN values
            row_dict = row.to_dict()
            # Remove NaN values for optional fields
            row_dict_clean = {
                k: v for k, v in row_dict.items()
                if not (pd.isna(v) and k in ['market_cap', 'rsi', 'change_pct', 'pe_ratio', 'asset_type', 'ingestion_timestamp'])
            }
            # Convert NaN to None for optional fields
            for k, v in row_dict_clean.items():
                if pd.isna(v):
                    row_dict_clean[k] = None
            
            schema_class(**row_dict_clean)
            valid_rows += 1
        except Exception as e:
            invalid_rows += 1
            error_msg = str(e)
            row_errors.append({
                'row': idx,
                'error': error_msg,
                'row_data': row_dict if 'row_dict' in locals() else row.to_dict()
            })
            
            if strict:
                errors.extend(row_errors)
                break
            else:
                errors.extend(row_errors)
    
    is_valid = invalid_rows == 0
    
    if errors:
        logger.warning(f"Validation found {len(errors)} errors in {invalid_rows} rows")
    
    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        total_rows=total_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows
    )


def validate_null_values(
    df: pd.DataFrame,
    required_columns: Optional[List[str]] = None,
    allow_null_columns: Optional[List[str]] = None
) -> ValidationResult:
    """
    Validate null values in DataFrame.
    
    Args:
        df: DataFrame to validate
        required_columns: List of columns that cannot have nulls (if None, uses default required columns)
        allow_null_columns: List of columns that are allowed to have nulls
    
    Returns:
        ValidationResult with null validation status
    """
    if df.empty:
        return ValidationResult(
            is_valid=False,
            errors=[{'error': 'DataFrame is empty'}],
            warnings=[],
            total_rows=0,
            valid_rows=0,
            invalid_rows=0
        )
    
    # Default required columns for market data
    if required_columns is None:
        required_columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    
    if allow_null_columns is None:
        allow_null_columns = ['market_cap', 'rsi', 'change_pct', 'pe_ratio', 'asset_type', 'ingestion_timestamp']
    
    errors = []
    warnings = []
    
    # Check for nulls in required columns
    for col in required_columns:
        if col not in df.columns:
            errors.append({
                'column': col,
                'error': f'Required column {col} is missing'
            })
            continue
        
        null_count = df[col].isna().sum()
        if null_count > 0:
            null_indices = df[df[col].isna()].index.tolist()
            errors.append({
                'column': col,
                'error': f'Column {col} has {null_count} null values',
                'null_count': null_count,
                'null_indices': null_indices[:10]  # Limit to first 10 for reporting
            })
    
    # Check for unexpected nulls in non-allowed columns
    for col in df.columns:
        if col not in required_columns and col not in allow_null_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                warnings.append({
                    'column': col,
                    'warning': f'Column {col} has {null_count} null values but is not in allow_null_columns',
                    'null_count': null_count
                })
    
    is_valid = len(errors) == 0
    invalid_rows = sum(1 for e in errors if 'null_indices' in e for _ in e['null_indices'])
    
    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        total_rows=len(df),
        valid_rows=len(df) - invalid_rows if invalid_rows > 0 else len(df),
        invalid_rows=invalid_rows
    )


def validate_data_types(
    df: pd.DataFrame,
    expected_types: Optional[Dict[str, type]] = None
) -> ValidationResult:
    """
    Validate data types of DataFrame columns.
    
    Args:
        df: DataFrame to validate
        expected_types: Dict mapping column names to expected types
    
    Returns:
        ValidationResult with type validation status
    """
    if df.empty:
        return ValidationResult(
            is_valid=False,
            errors=[{'error': 'DataFrame is empty'}],
            warnings=[],
            total_rows=0,
            valid_rows=0,
            invalid_rows=0
        )
    
    # Default expected types for market data
    if expected_types is None:
        expected_types = {
            'timestamp': datetime,
            'symbol': str,
            'open': (float, np.floating),
            'high': (float, np.floating),
            'low': (float, np.floating),
            'close': (float, np.floating),
            'volume': (int, np.integer),
            'market_cap': (int, np.integer, type(None)),
            'rsi': (float, np.floating, type(None)),
            'change_pct': (float, np.floating, type(None)),
        }
    
    errors = []
    warnings = []
    
    for col, expected_type in expected_types.items():
        if col not in df.columns:
            continue
        
        # Handle tuple of types (e.g., (float, np.floating))
        if isinstance(expected_type, tuple):
            type_names = [t.__name__ if hasattr(t, '__name__') else str(t) for t in expected_type]
        else:
            type_names = [expected_type.__name__ if hasattr(expected_type, '__name__') else str(expected_type)]
        
        actual_dtype = df[col].dtype
        
        # Check if actual type matches expected
        type_matches = False
        for exp_type in (expected_type if isinstance(expected_type, tuple) else (expected_type,)):
            if exp_type is type(None):
                # Allow None for optional fields
                continue
            if isinstance(exp_type, type):
                # Check pandas dtypes
                if exp_type == datetime and pd.api.types.is_datetime64_any_dtype(actual_dtype):
                    type_matches = True
                    break
                elif exp_type == str and pd.api.types.is_string_dtype(actual_dtype):
                    type_matches = True
                    break
                elif exp_type in (int, np.integer) and pd.api.types.is_integer_dtype(actual_dtype):
                    type_matches = True
                    break
                elif exp_type in (float, np.floating) and pd.api.types.is_float_dtype(actual_dtype):
                    type_matches = True
                    break
        
        if not type_matches:
            # Check if it's an optional field (None allowed)
            if type(None) in (expected_type if isinstance(expected_type, tuple) else (expected_type,)):
                # Allow if column can have nulls
                continue
            
            errors.append({
                'column': col,
                'error': f'Column {col} has type {actual_dtype} but expected {", ".join(type_names)}',
                'actual_type': str(actual_dtype),
                'expected_types': type_names
            })
    
    is_valid = len(errors) == 0
    
    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        total_rows=len(df),
        valid_rows=len(df) if is_valid else 0,
        invalid_rows=len(df) if not is_valid else 0
    )


def validate_business_rules(df: pd.DataFrame) -> ValidationResult:
    """
    Validate business rules for market data.
    
    Business rules:
    - high >= open, low, close
    - low <= open, high, close
    - open and close must be between low and high
    - volume >= 0
    - RSI between 0 and 100 if present
    - market_cap >= 0 if present
    
    Args:
        df: DataFrame to validate
    
    Returns:
        ValidationResult with business rule validation status
    """
    if df.empty:
        return ValidationResult(
            is_valid=False,
            errors=[{'error': 'DataFrame is empty'}],
            warnings=[],
            total_rows=0,
            valid_rows=0,
            invalid_rows=0
        )
    
    errors = []
    warnings = []
    invalid_rows = set()
    
    required_cols = ['open', 'high', 'low', 'close']
    if not all(col in df.columns for col in required_cols):
        return ValidationResult(
            is_valid=False,
            errors=[{'error': f'Missing required columns: {required_cols}'}],
            warnings=[],
            total_rows=len(df),
            valid_rows=0,
            invalid_rows=len(df)
        )
    
    # Rule 1: high >= low
    invalid_high_low = df[df['high'] < df['low']].index.tolist()
    for idx in invalid_high_low:
        errors.append({
            'row': idx,
            'error': f'high ({df.loc[idx, "high"]}) < low ({df.loc[idx, "low"]})',
            'rule': 'high >= low'
        })
        invalid_rows.add(idx)
    
    # Rule 2: open between low and high
    invalid_open = df[(df['open'] < df['low']) | (df['open'] > df['high'])].index.tolist()
    for idx in invalid_open:
        errors.append({
            'row': idx,
            'error': f'open ({df.loc[idx, "open"]}) not between low ({df.loc[idx, "low"]}) and high ({df.loc[idx, "high"]})',
            'rule': 'low <= open <= high'
        })
        invalid_rows.add(idx)
    
    # Rule 3: close between low and high
    invalid_close = df[(df['close'] < df['low']) | (df['close'] > df['high'])].index.tolist()
    for idx in invalid_close:
        errors.append({
            'row': idx,
            'error': f'close ({df.loc[idx, "close"]}) not between low ({df.loc[idx, "low"]}) and high ({df.loc[idx, "high"]})',
            'rule': 'low <= close <= high'
        })
        invalid_rows.add(idx)
    
    # Rule 4: volume >= 0
    if 'volume' in df.columns:
        invalid_volume = df[df['volume'] < 0].index.tolist()
        for idx in invalid_volume:
            errors.append({
                'row': idx,
                'error': f'volume ({df.loc[idx, "volume"]}) < 0',
                'rule': 'volume >= 0'
            })
            invalid_rows.add(idx)
    
    # Rule 5: RSI between 0 and 100 if present
    if 'rsi' in df.columns:
        invalid_rsi = df[(df['rsi'].notna()) & ((df['rsi'] < 0) | (df['rsi'] > 100))].index.tolist()
        for idx in invalid_rsi:
            errors.append({
                'row': idx,
                'error': f'rsi ({df.loc[idx, "rsi"]}) not between 0 and 100',
                'rule': '0 <= rsi <= 100'
            })
            invalid_rows.add(idx)
    
    # Rule 6: market_cap >= 0 if present
    if 'market_cap' in df.columns:
        invalid_market_cap = df[(df['market_cap'].notna()) & (df['market_cap'] < 0)].index.tolist()
        for idx in invalid_market_cap:
            errors.append({
                'row': idx,
                'error': f'market_cap ({df.loc[idx, "market_cap"]}) < 0',
                'rule': 'market_cap >= 0'
            })
            invalid_rows.add(idx)
    
    is_valid = len(errors) == 0
    
    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        total_rows=len(df),
        valid_rows=len(df) - len(invalid_rows),
        invalid_rows=len(invalid_rows)
    )


def detect_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = 'first'
) -> ValidationResult:
    """
    Detect duplicate rows in DataFrame.
    
    Args:
        df: DataFrame to check
        subset: List of columns to consider for duplicates (if None, checks all columns)
        keep: Which duplicates to mark ('first', 'last', False)
    
    Returns:
        ValidationResult with duplicate detection status
    """
    if df.empty:
        return ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            total_rows=0,
            valid_rows=0,
            invalid_rows=0
        )
    
    # Default subset for market data: timestamp + symbol should be unique
    if subset is None:
        subset = ['timestamp', 'symbol']
    
    # Check if subset columns exist
    missing_cols = [col for col in subset if col not in df.columns]
    if missing_cols:
        return ValidationResult(
            is_valid=False,
            errors=[{'error': f'Missing columns for duplicate check: {missing_cols}'}],
            warnings=[],
            total_rows=len(df),
            valid_rows=0,
            invalid_rows=len(df)
        )
    
    # Find duplicates
    duplicates = df.duplicated(subset=subset, keep=keep)
    duplicate_count = duplicates.sum()
    
    errors = []
    warnings = []
    
    if duplicate_count > 0:
        duplicate_indices = df[duplicates].index.tolist()
        errors.append({
            'error': f'Found {duplicate_count} duplicate rows based on columns: {subset}',
            'duplicate_count': duplicate_count,
            'duplicate_indices': duplicate_indices[:20],  # Limit to first 20 for reporting
            'subset': subset
        })
        logger.warning(f"Found {duplicate_count} duplicate rows")
    
    is_valid = duplicate_count == 0
    
    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        total_rows=len(df),
        valid_rows=len(df) - duplicate_count,
        invalid_rows=duplicate_count
    )
