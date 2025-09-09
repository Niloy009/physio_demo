"""
Data Access Layer for Physiotherapy Clinic Dashboard

This module provides clean, efficient functions to query the clinic database
for dashboard KPIs, ML model features, and business analytics.

Author: Niloy Saha Roy
Date: 09 September 2025
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd


class ClinicDataAccess:
    """
    Data access layer for clinic database operations.
    
    Provides methods to retrieve KPIs, patient analytics, and operational
    metrics for dashboard and ML model consumption.
    """

    def __init__(self, db_path: str = "data/clinic.db"):
        """
        Initialize data access connection.
        
        Args:
            db_path (str): Path to SQLite database file
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def get_real_time_kpis(self) -> Dict[str, Any]:
        """
        Get real-time KPIs for dashboard display.
        
        Returns:
            Dict[str, Any]: Dictionary containing key performance indicators
        """
        cursor = self.conn.cursor()

        # Today's metrics
        today = datetime.now().date()

        # Today's appointments
        cursor.execute("""
            SELECT COUNT(*) as total_today,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed_today,
                SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_today,
                SUM(CASE WHEN status = 'No-show' THEN 1 ELSE 0 END) as noshow_today
            FROM appointments 
            WHERE appointment_date = ?
        """, (today,))

        today_stats = cursor.fetchone()

        # This week's revenue
        week_start = today - timedelta(days=today.weekday())
        cursor.execute("""
            SELECT SUM(price) as weekly_revenue,
                COUNT(*) as weekly_appointments
            FROM appointments 
            WHERE appointment_date >= ? 
            AND appointment_date <= ?
            AND status = 'Completed'
        """, (week_start, today))

        weekly_stats = cursor.fetchone()

        # Therapist utilization (today)
        cursor.execute("""
            SELECT t.first_name || ' ' || t.last_name as therapist_name,
                t.max_patients_per_day,
                COUNT(a.appointment_id) as appointments_today,
                   ROUND(COUNT(a.appointment_id) * 100.0 / t.max_patients_per_day, 1) as utilization_rate
            FROM therapists t
            LEFT JOIN appointments a ON t.therapist_id = a.therapist_id 
                AND a.appointment_date = ? 
                AND a.status IN ('Completed', 'Scheduled', 'In-progress')
            WHERE t.is_active = 1
            GROUP BY t.therapist_id, t.first_name, t.last_name, t.max_patients_per_day
        """, (today,))

        therapist_utilization = [dict(row) for row in cursor.fetchall()]

        # Pending reception tasks
        cursor.execute("""
            SELECT COUNT(*) as pending_tasks,
                AVG(priority) as avg_priority
            FROM reception_tasks 
            WHERE status = 'Pending'
        """, )

        task_stats = cursor.fetchone()

        # Cancellation rate (last 30 days)
        thirty_days_ago = today - timedelta(days=30)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_appointments,
                SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_appointments,
                ROUND(SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cancellation_rate
            FROM appointments 
            WHERE appointment_date >= ?
        """, (thirty_days_ago,))

        cancellation_stats = cursor.fetchone()

        return {
            'today': {
                'total_appointments': today_stats['total_today'] or 0,
                'completed_appointments': today_stats['completed_today'] or 0,
                'cancelled_appointments': today_stats['cancelled_today'] or 0,
                'noshow_appointments': today_stats['noshow_today'] or 0
            },
            'weekly': {
                'revenue': weekly_stats['weekly_revenue'] or 0,
                'appointments': weekly_stats['weekly_appointments'] or 0,
                'avg_revenue_per_appointment': (weekly_stats['weekly_revenue'] or 0) / max(weekly_stats['weekly_appointments'] or 1, 1)
            },
            'therapist_utilization': therapist_utilization,
            'reception': {
                'pending_tasks': task_stats['pending_tasks'] or 0,
                'avg_priority': round(task_stats['avg_priority'] or 3, 1)
            },
            'cancellation_rate_30d': cancellation_stats['cancellation_rate'] or 0
        }

    def get_appointment_trends(self, days: int = 30) -> pd.DataFrame:
        """
        Get appointment trends over specified period.
        
        Args:
            days (int): Number of days to look back
            
        Returns:
            pd.DataFrame: Daily appointment statistics
        """
        start_date = datetime.now().date() - timedelta(days=days)

        query = """
        SELECT 
            appointment_date,
            COUNT(*) as total_appointments,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status = 'No-show' THEN 1 ELSE 0 END) as no_shows,
            SUM(CASE WHEN status = 'Completed' THEN price ELSE 0 END) as daily_revenue
        FROM appointments 
        WHERE appointment_date >= ?
        GROUP BY appointment_date 
        ORDER BY appointment_date
        """

        return pd.read_sql_query(query, self.conn, params=(start_date,))

    def get_therapist_performance(self) -> pd.DataFrame:
        """
        Get therapist performance metrics.
        
        Returns:
            pd.DataFrame: Therapist performance data
        """
        query = """
        SELECT 
            t.therapist_id,
            t.first_name || ' ' || t.last_name as therapist_name,
            t.specialization,
            COUNT(a.appointment_id) as total_appointments,
            SUM(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END) as completed_appointments,
            SUM(CASE WHEN a.status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_appointments,
            SUM(CASE WHEN a.status = 'No-show' THEN 1 ELSE 0 END) as noshow_appointments,
            ROUND(SUM(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.appointment_id), 1) as completion_rate,
            SUM(CASE WHEN a.status = 'Completed' THEN a.price ELSE 0 END) as total_revenue,
            ROUND(SUM(CASE WHEN a.status = 'Completed' THEN a.price ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END), 0), 2) as avg_revenue_per_session
        FROM therapists t
        LEFT JOIN appointments a ON t.therapist_id = a.therapist_id
        WHERE t.is_active = 1
        GROUP BY t.therapist_id, t.first_name, t.last_name, t.specialization
        ORDER BY total_revenue DESC
        """

        return pd.read_sql_query(query, self.conn)

    def get_patient_behavior_features(self) -> pd.DataFrame:
        """
        Get patient behavioral features for cancellation prediction ML model.
        
        Returns:
            pd.DataFrame: Patient features for ML model
        """
        query = """
        SELECT 
            p.patient_id,
            p.primary_condition,
            p.insurance_type,
            p.gender,
            ROUND((julianday('now') - julianday(p.date_of_birth)) / 365.25) as age,
            ROUND((julianday('now') - julianday(p.registration_date)) / 365.25, 1) as years_as_patient,
            
            -- Appointment history
            COUNT(a.appointment_id) as total_appointments,
            SUM(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END) as completed_appointments,
            SUM(CASE WHEN a.status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_appointments,
            SUM(CASE WHEN a.status = 'No-show' THEN 1 ELSE 0 END) as noshow_appointments,
            
            -- Behavioral patterns
            ROUND(SUM(CASE WHEN a.status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / 
                NULLIF(COUNT(a.appointment_id), 0), 2) as historical_cancellation_rate,
            
            -- Booking behavior
            SUM(CASE WHEN a.booking_method = 'Online' THEN 1 ELSE 0 END) as online_bookings,
            SUM(CASE WHEN a.booking_method = 'Phone' THEN 1 ELSE 0 END) as phone_bookings,
            
            -- Payment behavior
            AVG(CASE WHEN a.status = 'Completed' THEN a.price END) as avg_payment_amount,
            SUM(CASE WHEN a.insurance_covered = 1 THEN 1 ELSE 0 END) as insurance_claims,
            
            -- Recent activity
            MAX(a.appointment_date) as last_appointment_date,
            MIN(a.appointment_date) as first_appointment_date
            
        FROM patients p
        LEFT JOIN appointments a ON p.patient_id = a.patient_id
        GROUP BY p.patient_id, p.primary_condition, p.insurance_type, p.gender, 
                p.date_of_birth, p.registration_date
        HAVING COUNT(a.appointment_id) > 0
        """

        return pd.read_sql_query(query, self.conn)

    def get_cancellation_features(self) -> pd.DataFrame:
        """
        Get cancellation data with features for ML model training.
        
        Returns:
            pd.DataFrame: Cancellation data with features
        """
        query = """
        SELECT 
            a.appointment_id,
            a.patient_id,
            a.therapist_id,
            a.treatment_id,
            
            -- Appointment details
            CASE strftime('%w', a.appointment_date)
                WHEN '0' THEN 'Sunday'
                WHEN '1' THEN 'Monday' 
                WHEN '2' THEN 'Tuesday'
                WHEN '3' THEN 'Wednesday'
                WHEN '4' THEN 'Thursday'
                WHEN '5' THEN 'Friday'
                WHEN '6' THEN 'Saturday'
            END as day_of_week,
            CAST(strftime('%H', a.appointment_time) AS INTEGER) as appointment_hour,
            a.duration_minutes,
            a.price,
            a.booking_method,
            
            -- Patient info
            p.primary_condition,
            p.insurance_type,
            p.gender,
            ROUND((julianday(a.appointment_date) - julianday(p.date_of_birth)) / 365.25) as age_at_appointment,
            
            -- Booking timing
            ROUND((julianday(a.appointment_date) - julianday(a.booking_date))) as days_booked_in_advance,
            
            -- Treatment info
            t.treatment_name,
            t.category as treatment_category,
            
            -- Target variable
            CASE WHEN a.status = 'Cancelled' THEN 1 ELSE 0 END as was_cancelled,
            
            -- Cancellation details (if applicable)
            c.hours_before_appointment,
            c.reason_category,
            c.cancelled_by
            
        FROM appointments a
        JOIN patients p ON a.patient_id = p.patient_id
        JOIN treatments t ON a.treatment_id = t.treatment_id
        LEFT JOIN cancellations c ON a.appointment_id = c.appointment_id
        WHERE a.status IN ('Completed', 'Cancelled', 'No-show')
        ORDER BY a.appointment_date DESC
        """
        
        return pd.read_sql_query(query, self.conn)
    
    def get_reception_workflow_data(self) -> Dict[str, Any]:
        """
        Get reception workflow metrics for automation demo.
        
        Returns:
            Dict[str, Any]: Reception workflow statistics
        """
        cursor = self.conn.cursor()
        
        # Task completion metrics
        cursor.execute("""
            SELECT 
                task_type,
                COUNT(*) as total_tasks,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed_tasks,
                AVG(CASE WHEN status = 'Completed' THEN actual_duration_minutes END) as avg_completion_time,
                AVG(estimated_duration_minutes) as avg_estimated_time
            FROM reception_tasks 
            GROUP BY task_type
            ORDER BY total_tasks DESC
        """)
        
        task_metrics = [dict(row) for row in cursor.fetchall()]
        
        # Priority distribution
        cursor.execute("""
            SELECT 
                priority,
                COUNT(*) as task_count,
                AVG(CASE WHEN status = 'Completed' THEN actual_duration_minutes END) as avg_completion_time
            FROM reception_tasks 
            GROUP BY priority 
            ORDER BY priority
        """)
        
        priority_distribution = [dict(row) for row in cursor.fetchall()]
        
        # Daily task volume
        cursor.execute("""
            SELECT 
                DATE(created_at) as task_date,
                COUNT(*) as tasks_created,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as tasks_completed
            FROM reception_tasks 
            WHERE created_at >= date('now', '-30 days')
            GROUP BY DATE(created_at)
            ORDER BY task_date DESC
        """)
        
        daily_volume = [dict(row) for row in cursor.fetchall()]
        
        return {
            'task_metrics': task_metrics,
            'priority_distribution': priority_distribution,
            'daily_volume': daily_volume
        }
    
    def get_revenue_analytics(self, period_days: int = 30) -> Dict[str, Any]:
        """
        Get revenue analytics for business intelligence.
        
        Args:
            period_days (int): Number of days to analyze
            
        Returns:
            Dict[str, Any]: Revenue analytics data
        """
        start_date = datetime.now().date() - timedelta(days=period_days)
        cursor = self.conn.cursor()
        
        # Revenue by treatment type
        cursor.execute("""
            SELECT 
                t.treatment_name,
                t.category,
                COUNT(a.appointment_id) as session_count,
                SUM(a.price) as total_revenue,
                AVG(a.price) as avg_price_per_session
            FROM appointments a
            JOIN treatments t ON a.treatment_id = t.treatment_id
            WHERE a.appointment_date >= ? 
            AND a.status = 'Completed'
            GROUP BY t.treatment_id, t.treatment_name, t.category
            ORDER BY total_revenue DESC
        """, (start_date,))
        
        revenue_by_treatment = [dict(row) for row in cursor.fetchall()]
        
        # Revenue by therapist
        cursor.execute("""
            SELECT 
                th.first_name || ' ' || th.last_name as therapist_name,
                COUNT(a.appointment_id) as sessions_completed,
                SUM(a.price) as total_revenue,
                AVG(a.price) as avg_revenue_per_session
            FROM appointments a
            JOIN therapists th ON a.therapist_id = th.therapist_id
            WHERE a.appointment_date >= ? 
            AND a.status = 'Completed'
            GROUP BY th.therapist_id, th.first_name, th.last_name
            ORDER BY total_revenue DESC
        """, (start_date,))
        
        revenue_by_therapist = [dict(row) for row in cursor.fetchall()]
        
        # Insurance vs self-pay breakdown
        cursor.execute("""
            SELECT 
                CASE WHEN insurance_covered = 1 THEN 'Insurance' ELSE 'Self-Pay' END as payment_type,
                COUNT(*) as appointment_count,
                SUM(price) as total_revenue,
                AVG(price) as avg_payment
            FROM appointments 
            WHERE appointment_date >= ? 
            AND status = 'Completed'
            GROUP BY insurance_covered
        """, (start_date,))

        payment_breakdown = [dict(row) for row in cursor.fetchall()]

        return {
            'revenue_by_treatment': revenue_by_treatment,
            'revenue_by_therapist': revenue_by_therapist,
            'payment_breakdown': payment_breakdown,
            'period_days': period_days
        }

    def close(self) -> None:
        """Close database connection."""
        self.conn.close()


def main():
    """Test data access functions."""
    data_access = ClinicDataAccess()

    print("[INFO] Testing Data Access Functions...")

    # Test KPIs
    kpis = data_access.get_real_time_kpis()
    print(f"[INFO] Real-time KPIs: {kpis['today']['total_appointments']} appointments today")

    # Test trends
    trends = data_access.get_appointment_trends(7)
    print(f"[INFO] Appointment trends: {len(trends)} days of data")

    # Test ML features
    patient_features = data_access.get_patient_behavior_features()
    print(f"[INFO] Patient features: {len(patient_features)} patients with behavioral data")

    # Test cancellation data
    cancellation_data = data_access.get_cancellation_features()
    print(f"[INFO] Cancellation features: {len(cancellation_data)} appointment records")

    data_access.close()
    print("[INFO] All data access functions working correctly!")


if __name__ == "__main__":
    main()
