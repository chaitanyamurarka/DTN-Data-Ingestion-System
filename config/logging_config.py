"""
Enhanced Logging Configuration

Provides flexible logging with multiple handlers, structured logging support,
and environment-based configuration.
"""

import logging
import logging.handlers
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from enum import Enum


class LogLevel(Enum):
    """Logging level enumeration for cleaner configuration."""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured JSON logs for better parsing
    and analysis by log aggregation systems.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
            "process_id": os.getpid(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add any extra fields
        for key, value in record.__dict__.items():
            if key not in ["name", "msg", "args", "created", "filename", 
                          "funcName", "levelname", "levelno", "lineno", 
                          "module", "msecs", "pathname", "process", 
                          "processName", "relativeCreated", "thread", 
                          "threadName", "exc_info", "exc_text", "stack_info"]:
                log_data[key] = value
        
        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """
    Formatter that adds color to console output for better readability.
    """
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record: logging.LogRecord) -> str:
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


class LoggingConfig:
    """
    Centralized logging configuration manager with support for multiple
    handlers and environment-based settings.
    """
    
    def __init__(self, 
                 app_name: str = "dtn_ingestion",
                 log_dir: str = "./logs",
                 enable_console: bool = True,
                 enable_file: bool = True,
                 enable_json: bool = False,
                 log_level: str = None,
                 max_bytes: int = 100 * 1024 * 1024,  # 100MB
                 backup_count: int = 10,
                 enable_color: bool = True):
        
        self.app_name = app_name
        self.log_dir = Path(log_dir)
        self.enable_console = enable_console
        self.enable_file = enable_file
        self.enable_json = enable_json
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.enable_color = enable_color and sys.stdout.isatty()
        
        # Get log level from environment or parameter
        self.log_level = self._get_log_level(log_level)
        
        # Create log directory if it doesn't exist
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize handlers storage
        self._handlers = {}
    
    def _get_log_level(self, log_level: Optional[str]) -> int:
        """Get log level from parameter or environment variable."""
        level_str = log_level or os.getenv("LOG_LEVEL", "INFO")
        try:
            return LogLevel[level_str.upper()].value
        except KeyError:
            return LogLevel.INFO.value
    
    def _create_console_handler(self) -> logging.StreamHandler:
        """Create and configure console handler."""
        handler = logging.StreamHandler(sys.stdout)
        
        if self.enable_color:
            formatter = ColoredFormatter(
                '%(asctime)s - %(name)s - %(levelname)s - '
                '%(filename)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - '
                '%(filename)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        handler.setFormatter(formatter)
        handler.setLevel(self.log_level)
        return handler
    
    def _create_file_handler(self, filename: str) -> logging.handlers.RotatingFileHandler:
        """Create and configure rotating file handler."""
        file_path = self.log_dir / filename
        handler = logging.handlers.RotatingFileHandler(
            filename=str(file_path),
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - '
            '%(filename)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        handler.setFormatter(formatter)
        handler.setLevel(self.log_level)
        return handler
    
    def _create_json_handler(self) -> logging.handlers.RotatingFileHandler:
        """Create and configure JSON file handler for structured logging."""
        file_path = self.log_dir / f"{self.app_name}_json.log"
        handler = logging.handlers.RotatingFileHandler(
            filename=str(file_path),
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        
        handler.setFormatter(StructuredFormatter())
        handler.setLevel(self.log_level)
        return handler
    
    def _create_error_handler(self) -> logging.handlers.RotatingFileHandler:
        """Create dedicated error log handler."""
        file_path = self.log_dir / f"{self.app_name}_errors.log"
        handler = logging.handlers.RotatingFileHandler(
            filename=str(file_path),
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - '
            '%(filename)s:%(lineno)d - %(message)s\n'
            'Exception: %(exc_info)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        handler.setFormatter(formatter)
        handler.setLevel(logging.ERROR)
        return handler
    
    def setup_logger(self, 
                    logger_name: Optional[str] = None,
                    module_level: Optional[str] = None) -> logging.Logger:
        """
        Set up and configure a logger with the specified handlers.
        
        Args:
            logger_name: Name of the logger (defaults to app_name)
            module_level: Override log level for this specific logger
            
        Returns:
            Configured logger instance
        """
        logger_name = logger_name or self.app_name
        logger = logging.getLogger(logger_name)
        
        # Set module-specific level if provided
        if module_level:
            try:
                level = LogLevel[module_level.upper()].value
                logger.setLevel(level)
            except KeyError:
                logger.setLevel(self.log_level)
        else:
            logger.setLevel(self.log_level)
        
        # Clear existing handlers to prevent duplicates
        logger.handlers.clear()
        
        # Add console handler
        if self.enable_console:
            console_handler = self._create_console_handler()
            logger.addHandler(console_handler)
            self._handlers['console'] = console_handler
        
        # Add file handlers
        if self.enable_file:
            # Main log file
            file_handler = self._create_file_handler(f"{self.app_name}.log")
            logger.addHandler(file_handler)
            self._handlers['file'] = file_handler
            
            # Error log file
            error_handler = self._create_error_handler()
            logger.addHandler(error_handler)
            self._handlers['error'] = error_handler
        
        # Add JSON handler for structured logging
        if self.enable_json:
            json_handler = self._create_json_handler()
            logger.addHandler(json_handler)
            self._handlers['json'] = json_handler
        
        # Prevent propagation to root logger
        logger.propagate = False
        
        return logger
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a child logger with the same configuration.
        
        Args:
            name: Logger name (will be prefixed with app_name)
            
        Returns:
            Configured child logger
        """
        full_name = f"{self.app_name}.{name}"
        return logging.getLogger(full_name)
    
    def add_context_filter(self, logger: logging.Logger, context: Dict[str, Any]):
        """
        Add contextual information to all log records from a logger.
        
        Args:
            logger: Logger to add context to
            context: Dictionary of context values to add
        """
        class ContextFilter(logging.Filter):
            def filter(self, record):
                for key, value in context.items():
                    setattr(record, key, value)
                return True
        
        logger.addFilter(ContextFilter())
    
    def shutdown(self):
        """Properly close all handlers and flush logs."""
        for handler in self._handlers.values():
            handler.close()
        logging.shutdown()


# Convenience class for adding extra context to log messages
class LogContext:
    """Helper class for adding structured context to log messages."""
    
    @staticmethod
    def with_context(logger: logging.Logger, **kwargs):
        """
        Add extra context to a single log message.
        
        Example:
            LogContext.with_context(logger, user_id=123, action="login").info("User logged in")
        """
        class ContextLogger:
            def __init__(self, logger, context):
                self.logger = logger
                self.context = context
            
            def _log(self, level, msg, *args, **kwargs):
                extra = kwargs.get('extra', {})
                extra.update(self.context)
                kwargs['extra'] = extra
                getattr(self.logger, level)(msg, *args, **kwargs)
            
            def debug(self, msg, *args, **kwargs):
                self._log('debug', msg, *args, **kwargs)
            
            def info(self, msg, *args, **kwargs):
                self._log('info', msg, *args, **kwargs)
            
            def warning(self, msg, *args, **kwargs):
                self._log('warning', msg, *args, **kwargs)
            
            def error(self, msg, *args, **kwargs):
                self._log('error', msg, *args, **kwargs)
            
            def critical(self, msg, *args, **kwargs):
                self._log('critical', msg, *args, **kwargs)
        
        return ContextLogger(logger, kwargs)


# Default logging setup for backward compatibility
def setup_logging(app_name: str = "dtn_ingestion_logger",
                 log_dir: str = "./logs",
                 **kwargs) -> logging.Logger:
    """
    Setup logging with default configuration.
    
    Args:
        app_name: Name of the application/logger
        log_dir: Directory for log files
        **kwargs: Additional configuration options
        
    Returns:
        Configured logger instance
    """
    config = LoggingConfig(
        app_name=app_name,
        log_dir=log_dir,
        enable_json=kwargs.get('enable_json', False),
        log_level=kwargs.get('log_level'),
        enable_color=kwargs.get('enable_color', True)
    )
    
    return config.setup_logger()


# Create default logger instance
logger = setup_logging()


# Example usage and configuration for different services
if __name__ == "__main__":
    # Example 1: Basic usage with default logger
    logger.info("Application started")
    logger.debug("Debug information")
    logger.warning("Warning message")
    
    # Example 2: Service-specific logger with custom configuration
    tick_config = LoggingConfig(
        app_name="tick_ingestion",
        enable_json=True,
        log_level="DEBUG"
    )
    tick_logger = tick_config.setup_logger()
    
    # Example 3: Using structured logging with context
    LogContext.with_context(
        tick_logger,
        symbol="AAPL",
        exchange="NASDAQ",
        service="live_tick"
    ).info("Processing tick data")
    
    # Example 4: Module-specific logger
    db_logger = tick_config.get_logger("database")
    db_logger.info("Database connection established")
    
    # Example 5: Error logging with exception
    try:
        1 / 0
    except Exception as e:
        logger.error("An error occurred", exc_info=True)