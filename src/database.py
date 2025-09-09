"""
database.py

Module for managing the SQLite database of a physiotherapy clinic management system.

Features:
- Defines the ClinicDatabase class for handling database connection, table creation, and sample data insertion.
- Supports tables for patients, therapists, treatments, appointments, cancellations, reception tasks, and patient history.
- Ensures the database directory exists and provides utility methods for connection management.
- Includes a test block for verifying table creation and sample data insertion.

"""

import sqlite3
import os

class ClinicDatabase:
    """
    Handles SQLite database operations for a physiotherapy clinic management system.

    Responsibilities:
    - Initializes and connects to the database.
    - Creates tables for patients, therapists, treatments, appointments, cancellations, 
    reception tasks, and patient history.
    - Provides methods for inserting sample data and accessing the database connection.
    - Ensures the data directory exists before creating the database file.
    """
    def __init__(self, db_path="data/clinic.db"):
        """Initialize database connection and create tables if they don't exist"""
        # Ensure data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Connect to SQLite database
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        self.create_tables()

    def create_tables(self):
        """Create all database tables"""
        cursor = self.conn.cursor()

        # 1. PATIENTS TABLE
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS patients (
                patient_id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                phone TEXT NOT NULL,
                date_of_birth DATE NOT NULL,
                gender TEXT CHECK (gender IN ('M', 'F', 'Other')),
                address TEXT,
                insurance_type TEXT CHECK (insurance_type IN ('Public', 'Private', 'Self-pay')),
                primary_condition TEXT,
                registration_date DATE NOT NULL,
                emergency_contact TEXT,
                emergency_phone TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 2. THERAPISTS TABLE
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS therapists (
                therapist_id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                phone TEXT NOT NULL,
                specialization TEXT NOT NULL,
                license_number TEXT UNIQUE NOT NULL,
                hire_date DATE NOT NULL,
                hourly_rate DECIMAL(8,2),
                max_patients_per_day INTEGER DEFAULT 12,
                working_days TEXT DEFAULT 'Mon,Tue,Wed,Thu,Fri',
                start_time TIME DEFAULT '08:00',
                end_time TIME DEFAULT '17:00',
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 3. TREATMENTS TABLE
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS treatments (
                treatment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                treatment_name TEXT NOT NULL,
                treatment_code TEXT UNIQUE NOT NULL,
                duration_minutes INTEGER NOT NULL,
                base_price DECIMAL(8,2) NOT NULL,
                description TEXT,
                requires_equipment BOOLEAN DEFAULT 0,
                category TEXT CHECK (category IN ('Manual Therapy', 'Exercise Therapy', 'Electrotherapy', 'Hydrotherapy', 'Assessment')),
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 4. APPOINTMENTS TABLE
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS appointments (
                appointment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                therapist_id INTEGER NOT NULL,
                treatment_id INTEGER NOT NULL,
                appointment_date DATE NOT NULL,
                appointment_time TIME NOT NULL,
                duration_minutes INTEGER NOT NULL,
                status TEXT CHECK (status IN ('Scheduled', 'Completed', 'Cancelled', 'No-show', 'In-progress')) DEFAULT 'Scheduled',
                booking_date TIMESTAMP NOT NULL,
                booking_method TEXT CHECK (booking_method IN ('Online', 'Phone', 'Walk-in', 'Referral')) DEFAULT 'Phone',
                price DECIMAL(8,2) NOT NULL,
                insurance_covered BOOLEAN DEFAULT 0,
                copay_amount DECIMAL(8,2) DEFAULT 0,
                notes TEXT,
                reminder_sent BOOLEAN DEFAULT 0,
                check_in_time TIMESTAMP,
                treatment_start_time TIMESTAMP,
                treatment_end_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (patient_id),
                FOREIGN KEY (therapist_id) REFERENCES therapists (therapist_id),
                FOREIGN KEY (treatment_id) REFERENCES treatments (treatment_id)
            )
        ''')

        # 5. CANCELLATIONS TABLE
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cancellations (
                cancellation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                appointment_id INTEGER NOT NULL,
                cancelled_by TEXT CHECK (cancelled_by IN ('Patient', 'Clinic', 'Therapist', 'System')),
                cancellation_date TIMESTAMP NOT NULL,
                hours_before_appointment INTEGER,
                reason_category TEXT CHECK (reason_category IN ('Personal', 'Medical', 'Transportation', 'Work', 'Weather', 'Other')),
                reason_detail TEXT,
                refund_issued BOOLEAN DEFAULT 0,
                refund_amount DECIMAL(8,2) DEFAULT 0,
                rescheduled BOOLEAN DEFAULT 0,
                new_appointment_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (appointment_id) REFERENCES appointments (appointment_id),
                FOREIGN KEY (new_appointment_id) REFERENCES appointments (appointment_id)
            )
        ''')

        # 6. RECEPTION_TASKS TABLE (for workflow automation demo)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reception_tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT CHECK (task_type IN ('Appointment Confirmation', 'Insurance Verification', 'Payment Processing', 'Patient Check-in', 'Reminder Call', 'Follow-up')),
                patient_id INTEGER,
                appointment_id INTEGER,
                priority INTEGER CHECK (priority IN (1, 2, 3, 4, 5)) DEFAULT 3,
                status TEXT CHECK (status IN ('Pending', 'In Progress', 'Completed', 'Cancelled')) DEFAULT 'Pending',
                assigned_to TEXT,
                estimated_duration_minutes INTEGER,
                actual_duration_minutes INTEGER,
                due_date TIMESTAMP,
                completed_date TIMESTAMP,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (patient_id),
                FOREIGN KEY (appointment_id) REFERENCES appointments (appointment_id)
            )
        ''')

        # 7. PATIENT_HISTORY TABLE (for tracking patient behavior patterns)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS patient_history (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                total_appointments INTEGER DEFAULT 0,
                completed_appointments INTEGER DEFAULT 0,
                cancelled_appointments INTEGER DEFAULT 0,
                no_show_appointments INTEGER DEFAULT 0,
                avg_cancellation_hours DECIMAL(8,2),
                last_appointment_date DATE,
                next_appointment_date DATE,
                avg_time_between_appointments DECIMAL(8,2),
                preferred_therapist_id INTEGER,
                preferred_time_slot TEXT,
                payment_history_score INTEGER DEFAULT 100,
                communication_preference TEXT CHECK (communication_preference IN ('Email', 'SMS', 'Phone', 'None')),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (patient_id),
                FOREIGN KEY (preferred_therapist_id) REFERENCES therapists (therapist_id)
            )
        ''')

        # Commit changes
        self.conn.commit()
        print("[INFO] All database tables created successfully!")

    def get_connection(self):
        """Return database connection"""
        return self.conn

    def close(self):
        """Close database connection"""
        self.conn.close()

    def insert_sample_treatments(self):
        """Insert common physiotherapy treatments"""
        treatments = [
            ('Initial Assessment', 'ASSESS-001', 60, 85.00, 'Comprehensive initial evaluation and treatment planning', 0, 'Assessment'),
            ('Manual Therapy', 'MT-001', 45, 75.00, 'Hands-on techniques including mobilization and manipulation', 0, 'Manual Therapy'),
            ('Exercise Therapy', 'ET-001', 45, 65.00, 'Supervised therapeutic exercises and movement training', 1, 'Exercise Therapy'),
            ('Sports Massage', 'SM-001', 30, 55.00, 'Deep tissue massage for athletes and active individuals', 0, 'Manual Therapy'),
            ('Electrotherapy', 'ELECTRO-001', 30, 45.00, 'TENS, ultrasound, and electrical stimulation', 1, 'Electrotherapy'),
            ('Dry Needling', 'DN-001', 30, 70.00, 'Trigger point dry needling for muscle pain relief', 1, 'Manual Therapy'),
            ('Postural Correction', 'PC-001', 45, 65.00, 'Assessment and correction of postural imbalances', 0, 'Exercise Therapy'),
            ('Balance Training', 'BT-001', 30, 50.00, 'Proprioceptive and balance enhancement exercises', 1, 'Exercise Therapy'),
            ('Hydrotherapy', 'HYDRO-001', 45, 80.00, 'Water-based rehabilitation and exercise', 1, 'Hydrotherapy'),
            ('Follow-up Session', 'FU-001', 30, 60.00, 'Progress review and treatment adjustment', 0, 'Assessment')
        ]

        cursor = self.conn.cursor()
        cursor.executemany('''
            INSERT OR IGNORE INTO treatments 
            (treatment_name, treatment_code, duration_minutes, base_price, description, requires_equipment, category)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', treatments)

        self.conn.commit()
        print("[INFO] Sample treatments inserted successfully!")

# Test the database setup
if __name__ == "__main__":
    # Create database and tables
    db = ClinicDatabase()

    # Insert sample treatments
    db.insert_sample_treatments()

    # Test connection
    test_cursor = db.get_connection().cursor()
    test_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = test_cursor.fetchall()

    print(f"\n[INFO] Created {len(tables)} tables:")
    for table in tables:
        print(f"  • {table[0]}")

    db.close()
    print("\n[INFO] Database setup complete!")
