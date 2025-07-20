import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, date
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

# Import your service functions
from job_scheduler.services.job_service import (
    schedule_new_job,
    fetch_scheduled_jobs,
    fetch_job_details
)


class TestJobService:
    """Simple unit tests for job service functions with 100% coverage"""

    def test_schedule_new_job_success(self, monkeypatch):
        """Test successful job scheduling"""
        # Mock dependencies
        mock_db = Mock(spec=Session)
        mock_job_instance = Mock()
        mock_job_instance.id = 123
        mock_job_instance.jobname = "test_job"
        mock_job_instance.frequency = "daily"
        mock_job_instance.schedule_time = "10:00:00"
        mock_job_instance.start_date = date.today()
        mock_job_instance.end_date = date.today()
        
        mock_job_request = Mock()
        mock_job_request.model_dump.return_value = {"jobname": "test_job"}
        
        # Monkeypatch dependencies
        mock_job_class = Mock(return_value=mock_job_instance)
        monkeypatch.setattr('job_scheduler.services.job_service.Job', mock_job_class)
        monkeypatch.setattr('job_scheduler.services.job_service.JobScheduler.add_job', Mock())
        monkeypatch.setattr('job_scheduler.services.job_service.ScheduledJobResponse', Mock())
        
        # Execute
        schedule_new_job(mock_job_request, mock_db)
        
        # Verify calls
        mock_db.add.assert_called_once_with(mock_job_instance)
        mock_db.commit.assert_called_once()
        mock_db.refresh.assert_called_once_with(mock_job_instance)

    def test_schedule_new_job_exception_with_successful_rollback(self, monkeypatch):
        """Test exception handling with successful rollback"""
        mock_db = Mock(spec=Session)
        mock_job_request = Mock()
        mock_job_request.model_dump.return_value = {}
        mock_logger = Mock()
        
        # Mock Job to raise exception
        monkeypatch.setattr('job_scheduler.services.job_service.Job', Mock(side_effect=Exception("test error")))
        monkeypatch.setattr('job_scheduler.services.job_service.logger', mock_logger)
        
        # Execute and verify exception
        with pytest.raises(HTTPException) as exc_info:
            schedule_new_job(mock_job_request, mock_db)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        mock_db.rollback.assert_called_once()

    def test_schedule_new_job_exception_with_rollback_failure(self, monkeypatch):
        """Test exception handling when rollback also fails"""
        mock_db = Mock(spec=Session)
        mock_db.rollback.side_effect = Exception("rollback failed")
        mock_job_request = Mock()
        mock_job_request.model_dump.return_value = {}
        mock_logger = Mock()
        
        # Mock Job to raise exception
        monkeypatch.setattr('job_scheduler.services.job_service.Job', Mock(side_effect=Exception("test error")))
        monkeypatch.setattr('job_scheduler.services.job_service.logger', mock_logger)
        
        # Execute and verify exception
        with pytest.raises(HTTPException):
            schedule_new_job(mock_job_request, mock_db)
        
        # Verify rollback error was logged
        mock_logger.error.assert_called()

    def test_fetch_scheduled_jobs_success(self, monkeypatch):
        """Test successful fetching of scheduled jobs"""
        mock_db = Mock(spec=Session)
        mock_job1 = Mock()
        mock_job1.id = 1
        mock_job1.jobname = "job1"
        mock_job1.frequency = "daily"
        mock_job1.schedule_time = "10:00"
        mock_job1.start_date = date.today()
        mock_job1.end_date = date.today()
        
        mock_job2 = Mock()
        mock_job2.id = 2
        mock_job2.jobname = "job2"
        mock_job2.frequency = "weekly"
        mock_job2.schedule_time = "14:00"
        mock_job2.start_date = date.today()
        mock_job2.end_date = date.today()
        
        mock_query = Mock()
        mock_query.all.return_value = [mock_job1, mock_job2]
        mock_db.query.return_value = mock_query
        
        # Mock ScheduledJobResponse
        mock_response_class = Mock()
        monkeypatch.setattr('job_scheduler.services.job_service.ScheduledJobResponse', mock_response_class)
        
        # Execute
        result = fetch_scheduled_jobs(mock_db)
        
        # Verify
        mock_db.query.assert_called_once()
        mock_query.all.assert_called_once()
        assert mock_response_class.call_count == 2

    def test_fetch_scheduled_jobs_exception(self, monkeypatch):
        """Test exception handling in fetch_scheduled_jobs"""
        mock_db = Mock(spec=Session)
        mock_db.query.side_effect = Exception("database error")
        mock_logger = Mock()
        
        monkeypatch.setattr('job_scheduler.services.job_service.logger', mock_logger)
        
        # Execute and verify exception
        with pytest.raises(HTTPException) as exc_info:
            fetch_scheduled_jobs(mock_db)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        mock_logger.error.assert_called()

    def test_fetch_job_details_success_with_status(self, monkeypatch):
        """Test successful job details fetch with job status"""
        mock_db = Mock(spec=Session)
        job_id = 123
        
        # Mock job
        mock_job = Mock()
        mock_job.id = job_id
        mock_job.jobname = "test_job"
        mock_job.user_id = 1
        
        # Mock job status
        mock_job_status = Mock()
        mock_job_status.status = "completed"
        mock_job_status.execution_log = "success"
        mock_job_status.start_time = datetime.now()
        
        # Mock database queries
        mock_job_query = Mock()
        mock_job_filter = Mock()
        mock_job_filter.first.return_value = mock_job
        mock_job_query.filter.return_value = mock_job_filter
        
        mock_status_query = Mock()
        mock_status_filter = Mock()
        mock_status_filter.first.return_value = mock_job_status
        mock_status_query.filter.return_value = mock_status_filter
        
        # Setup query side effect
        call_count = 0
        def query_side_effect(model):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_job_query
            else:
                return mock_status_query
        
        mock_db.query.side_effect = query_side_effect
        mock_logger = Mock()
        monkeypatch.setattr('job_scheduler.services.job_service.logger', mock_logger)
        
        # Execute
        result = fetch_job_details(job_id, mock_db)
        
        # Verify
        assert result["id"] == job_id
        assert result["jobname"] == "test_job"
        assert result["status"] == "completed"
        mock_logger.info.assert_called()

    def test_fetch_job_details_success_no_status(self, monkeypatch):
        """Test successful job details fetch without job status"""
        mock_db = Mock(spec=Session)
        job_id = 123
        
        # Mock job
        mock_job = Mock()
        mock_job.id = job_id
        mock_job.jobname = "test_job"
        mock_job.user_id = 1
        
        # Mock database queries
        mock_job_query = Mock()
        mock_job_filter = Mock()
        mock_job_filter.first.return_value = mock_job
        mock_job_query.filter.return_value = mock_job_filter
        
        mock_status_query = Mock()
        mock_status_filter = Mock()
        mock_status_filter.first.return_value = None  # No status found
        mock_status_query.filter.return_value = mock_status_filter
        
        # Setup query side effect
        call_count = 0
        def query_side_effect(model):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_job_query
            else:
                return mock_status_query
        
        mock_db.query.side_effect = query_side_effect
        mock_logger = Mock()
        monkeypatch.setattr('job_scheduler.services.job_service.logger', mock_logger)
        
        # Execute
        result = fetch_job_details(job_id, mock_db)
        
        # Verify default values are used
        assert result["id"] == job_id
        assert result["status"] == "No status available"
        assert result["execution_log"] == "No logs available"

    def test_fetch_job_details_job_not_found(self, monkeypatch):
        """Test job not found scenario"""
        mock_db = Mock(spec=Session)
        job_id = 999
        
        # Mock job query to return None
        mock_job_query = Mock()
        mock_job_filter = Mock()
        mock_job_filter.first.return_value = None
        mock_job_query.filter.return_value = mock_job_filter
        mock_db.query.return_value = mock_job_query
        
        # Execute and verify 404 exception
        with pytest.raises(HTTPException) as exc_info:
            fetch_job_details(job_id, mock_db)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    def test_fetch_job_details_exception(self, monkeypatch):
        """Test exception handling in fetch_job_details"""
        mock_db = Mock(spec=Session)
        mock_db.query.side_effect = Exception("database error")
        mock_logger = Mock()
        
        monkeypatch.setattr('job_scheduler.services.job_service.logger', mock_logger)
        
        # Execute and verify exception
        with pytest.raises(HTTPException) as exc_info:
            fetch_job_details(123, mock_db)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        mock_logger.error.assert_called()
