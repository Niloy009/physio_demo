"""
Data Generator for Physiotherapy Clinic Demo

This module generates realistic sample data for a physiotherapy clinic including:
- Patient demographics and medical history
- Therapist profiles and schedules
- Appointment bookings with realistic patterns
- Cancellation behaviors for ML modeling
- Reception workflow tasks

Author: Niloy Saha Roy
Date: 09 September 2025
"""

import random
import sqlite3
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
from faker import Faker
from database import ClinicDatabase


class ClinicDataGenerator:
    """
    Generates realistic physiotherapy clinic data for demo purposes.
    
    This class creates synthetic but realistic data patterns that mirror
    actual clinic operations, including patient behavior, scheduling patterns,
    and operational workflows.
    """
    def __init__(self, db_path: str = "data/clinic.db", locale: str = "de_DE"):
        """
        Initialize the data generator.
        
        Args:
            db_path (str): Path to SQLite database file
            locale (str): Faker locale for generating realistic names/addresses
        """
        self.db_path = db_path
        self.fake = Faker(locale)
        self.conn = sqlite3.connect(db_path)

        # Business logic constants
        self.CLINIC_START_DATE = datetime.now() - timedelta(days=180)  # 6 months ago
        self.CLINIC_END_DATE = datetime.now() + timedelta(days=30)     # 1 month future

        # Realistic clinical parameters
        self.PATIENT_AGE_WEIGHTS = {
            (25, 35): 0.15,  # Young adults
            (35, 50): 0.25,  # Peak physio age group
            (50, 65): 0.35,  # Most common patients
            (65, 80): 0.20,  # Elderly
            (80, 90): 0.05   # Very elderly
        }

        self.CONDITION_DISTRIBUTION = {
            'Lower Back Pain': 0.25,
            'Neck Pain': 0.15,
            'Knee Injury': 0.12,
            'Shoulder Pain': 0.10,
            'Sports Injury': 0.08,
            'Post-Surgery Rehab': 0.07,
            'Arthritis Management': 0.06,
            'Postural Problems': 0.05,
            'Sciatica': 0.04,
            'Tennis Elbow': 0.03,
            'Fibromyalgia': 0.02,
            'Other': 0.03
        }

        self.CANCELLATION_REASONS = {
            'Personal': 0.30,
            'Medical': 0.20,
            'Work': 0.18,
            'Transportation': 0.12,
            'Weather': 0.10,
            'Other': 0.10
        }

    def _weighted_choice(self, choices: Dict[Any, float]) -> Any:
        """
        Make a weighted random choice from a dictionary.
        
        Args:
            choices (Dict[Any, float]): Dictionary with choices as keys and weights as values
            
        Returns:
            Any: Randomly selected choice based on weights
        """
        items, weights = zip(*choices.items())
        return random.choices(items, weights=weights)[0]

    def _generate_realistic_age(self) -> int:
        """
        Generate realistic patient age based on physiotherapy demographics.
        
        Returns:
            int: Patient age between 25-90
        """
        age_range = self._weighted_choice(self.PATIENT_AGE_WEIGHTS)
        return random.randint(age_range[0], age_range[1])

    def _get_business_hours_datetime(self, date: datetime) -> datetime:
        """
        Generate realistic appointment time during business hours.
        
        Args:
            date (datetime): Base date for appointment
            
        Returns:
            datetime: Appointment datetime during business hours (8:00-17:00)
        """
        # Business hours: 8:00 AM to 5:00 PM
        # Peak hours: 9:00 AM - 12:00 PM and 2:00 PM - 4:00 PM
        peak_morning = list(range(9, 12))
        peak_afternoon = list(range(14, 16))
        regular_hours = list(range(8, 9)) + list(range(12, 14)) + list(range(16, 17))

        # 70% chance of peak hours, 30% regular hours
        if random.random() < 0.7:
            hour_pool = peak_morning + peak_afternoon
        else:
            hour_pool = regular_hours

        hour = random.choice(hour_pool)
        minute = random.choice([0, 15, 30, 45])  # 15-minute intervals

        return date.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def generate_patients(self, num_patients: int = 500) -> List[Dict[str, Any]]:
        """
        Generate realistic patient profiles.
        
        Args:
            num_patients (int): Number of patient records to generate
            
        Returns:
            List[Dict[str, Any]]: List of patient dictionaries
        """
        patients = []

        for _ in range(num_patients):
            age = self._generate_realistic_age()
            gender = random.choice(['M', 'F'])

            # Generate birth date based on age
            birth_date = datetime.now().date() - timedelta(days=age * 365)

            # Registration date (when they first became a patient)
            reg_days_ago = random.randint(30, 180)
            registration_date = datetime.now().date() - timedelta(days=reg_days_ago)

            patient = {
                'first_name': self.fake.first_name_male() if gender == 'M' else self.fake.first_name_female(),
                'last_name': self.fake.last_name(),
                'email': self.fake.unique.email(),
                'phone': self.fake.phone_number(),
                'date_of_birth': birth_date,
                'gender': gender,
                'address': self.fake.address().replace('\n', ', '),
                'insurance_type': self._weighted_choice({
                    'Public': 0.70,
                    'Private': 0.25,
                    'Self-pay': 0.05
                }),
                'primary_condition': self._weighted_choice(self.CONDITION_DISTRIBUTION),
                'registration_date': registration_date,
                'emergency_contact': self.fake.name(),
                'emergency_phone': self.fake.phone_number(),
                'notes': self.fake.text(max_nb_chars=100) if random.random() < 0.3 else None
            }

            patients.append(patient)

        return patients

    def generate_therapists(self, num_therapists: int = 6) -> List[Dict[str, Any]]:
        """
        Generate realistic therapist profiles with specializations.
        
        Args:
            num_therapists (int): Number of therapist records to generate
            
        Returns:
            List[Dict[str, Any]]: List of therapist dictionaries
        """
        specializations = [
            'Orthopedic Physical Therapy',
            'Sports Physical Therapy',
            'Neurological Physical Therapy',
            'Geriatric Physical Therapy',
            'Manual Therapy',
            'Pediatric Physical Therapy'
        ]

        therapists = []

        for i in range(num_therapists):
            # Vary working schedules for realism
            if i < 4:  # Full-time therapists
                working_days = 'Mon,Tue,Wed,Thu,Fri'
                start_time = '08:00'
                end_time = '17:00'
                max_patients = 12
            elif i == 4:  # Part-time morning
                working_days = 'Mon,Wed,Fri'
                start_time = '08:00'
                end_time = '13:00'
                max_patients = 6
            else:  # Part-time afternoon
                working_days = 'Tue,Thu,Fri'
                start_time = '13:00'
                end_time = '18:00'
                max_patients = 6

            hire_days_ago = random.randint(365, 1800)  # 1-5 years experience
            hire_date = datetime.now().date() - timedelta(days=hire_days_ago)

            therapist = {
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.unique.email(),
                'phone': self.fake.phone_number(),
                'specialization': specializations[i % len(specializations)], 
                'license_number': f"PT{random.randint(10000, 99999)}",
                'hire_date': hire_date,
                'hourly_rate': round(random.uniform(35.0, 55.0), 2),
                'max_patients_per_day': max_patients,
                'working_days': working_days,
                'start_time': start_time,
                'end_time': end_time,
                'is_active': 1
            }

            therapists.append(therapist)

        return therapists

    def generate_appointments(self,
                            patient_ids: List[int],
                            therapist_ids: List[int],
                            treatment_ids: List[int],
                            num_appointments: int = 3000) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Generate realistic appointment and cancellation records.
        
        Args:
            patient_ids (List[int]): List of valid patient IDs
            therapist_ids (List[int]): List of valid therapist IDs
            treatment_ids (List[int]): List of valid treatment IDs
            num_appointments (int): Number of appointments to generate
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (appointments, cancellations)
        """
        appointments = []
        cancellations = []

        # Get treatment durations and prices from database
        cursor = self.conn.cursor()
        cursor.execute("SELECT treatment_id, duration_minutes, base_price FROM treatments")
        treatment_info = {row[0]: {'duration': row[1], 'price': row[2]} for row in cursor.fetchall()}

        for appointment_id in range(1, num_appointments + 1):
            # Select random entities
            patient_id = random.choice(patient_ids)
            therapist_id = random.choice(therapist_ids)
            treatment_id = random.choice(treatment_ids)

            # Generate appointment date (weighted towards recent dates)
            days_from_start = int(random.triangular(0, 180, 120))  # Bias towards recent
            appointment_date = self.CLINIC_START_DATE + timedelta(days=days_from_start)

            # Skip weekends for most appointments
            if appointment_date.weekday() >= 5 and random.random() < 0.9:
                appointment_date += timedelta(days=2)

            appointment_datetime = self._get_business_hours_datetime(appointment_date)

            # Booking was made 1-14 days before appointment
            booking_date = appointment_datetime - timedelta(days=random.randint(1, 14))

            # Treatment details
            duration = treatment_info[treatment_id]['duration']
            base_price = treatment_info[treatment_id]['price']

            # Insurance coverage affects pricing
            insurance_covered = random.choice([True, False])
            if insurance_covered:
                copay_amount = round(base_price * random.uniform(0.1, 0.3), 2)
                final_price = copay_amount
            else:
                copay_amount = 0
                final_price = base_price

            # Determine appointment status with realistic probabilities
            status_weights = {
                'Completed': 0.75,
                'Cancelled': 0.15,
                'No-show': 0.07,
                'Scheduled': 0.03  # Future appointments
            }

            # Future appointments are always "Scheduled"
            if appointment_datetime > datetime.now():
                status = 'Scheduled'
            else:
                status = self._weighted_choice(status_weights)

            appointment = {
                'appointment_id': appointment_id,
                'patient_id': patient_id,
                'therapist_id': therapist_id,
                'treatment_id': treatment_id,
                'appointment_date': appointment_datetime.date(),
                'appointment_time': appointment_datetime.time(),
                'duration_minutes': duration,
                'status': status,
                'booking_date': booking_date,
                'booking_method': self._weighted_choice({
                    'Phone': 0.60,
                    'Online': 0.25,
                    'Walk-in': 0.10,
                    'Referral': 0.05
                }),
                'price': final_price,
                'insurance_covered': insurance_covered,
                'copay_amount': copay_amount,
                'notes': self.fake.sentence() if random.random() < 0.2 else None,
                'reminder_sent': random.choice([True, False])
            }

            # Add treatment timestamps for completed appointments
            if status == 'Completed':
                check_in_time = appointment_datetime - timedelta(minutes=random.randint(5, 15))
                treatment_start = appointment_datetime + timedelta(minutes=random.randint(0, 10))
                treatment_end = treatment_start + timedelta(minutes=duration + random.randint(-5, 15))

                appointment.update({
                    'check_in_time': check_in_time,
                    'treatment_start_time': treatment_start,
                    'treatment_end_time': treatment_end
                })

            appointments.append(appointment)

            # Generate cancellation record if appointment was cancelled
            if status == 'Cancelled':
                cancellation = self._generate_cancellation(appointment_id, appointment_datetime)
                cancellations.append(cancellation)

        return appointments, cancellations

    def _generate_cancellation(self, appointment_id: int, appointment_datetime: datetime) -> Dict[str, Any]:
        """
        Generate realistic cancellation record.
        
        Args:
            appointment_id (int): ID of cancelled appointment
            appointment_datetime (datetime): Original appointment time
            
        Returns:
            Dict[str, Any]: Cancellation record
        """
        # Cancellation timing patterns
        hours_before_weights = {
            1: 0.15,    # Last minute (< 1 hour)
            4: 0.20,    # Same day (1-4 hours)
            24: 0.30,   # Day before (4-24 hours)
            48: 0.20,   # 1-2 days before
            168: 0.15   # More than 2 days before
        }

        max_hours_before = self._weighted_choice(hours_before_weights)
        hours_before = random.randint(1, max_hours_before)
        cancellation_date = appointment_datetime - timedelta(hours=hours_before)

        reason_category = self._weighted_choice(self.CANCELLATION_REASONS)

        # Generate specific reason based on category
        reason_details = {
            'Personal': ['Family emergency', 'Personal commitment', 'Childcare issues', 'Travel conflict'],
            'Medical': ['Feeling unwell', 'Other medical appointment', 'Injury worsened', 'Doctor advised rest'],
            'Work': ['Work meeting', 'Unable to leave work', 'Business travel', 'Shift change'],
            'Transportation': ['Car trouble', 'Public transport delay', 'Traffic jam', 'No ride available'],
            'Weather': ['Heavy rain', 'Snow storm', 'Icy roads', 'Severe weather warning'],
            'Other': ['Forgot appointment', 'Double booked', 'Financial reasons', 'No longer needed']
        }

        reason_detail = random.choice(reason_details[reason_category])

        # Refund policy (within 24 hours = no refund for many clinics)
        if hours_before >= 24:
            refund_issued = random.choice([True, False])
            refund_amount = 25.0 if refund_issued else 0.0
        else:
            refund_issued = False
            refund_amount = 0.0

        return {
            'appointment_id': appointment_id,
            'cancelled_by': self._weighted_choice({
                'Patient': 0.85,
                'Clinic': 0.10,
                'Therapist': 0.05
            }),
            'cancellation_date': cancellation_date,
            'hours_before_appointment': hours_before,
            'reason_category': reason_category,
            'reason_detail': reason_detail,
            'refund_issued': refund_issued,
            'refund_amount': refund_amount,
            'rescheduled': random.choice([True, False])
        }

    def generate_reception_tasks(self, appointment_ids: List[int], patient_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Generate reception workflow tasks for automation demo.
        
        Args:
            appointment_ids (List[int]): Valid appointment IDs
            patient_ids (List[int]): Valid patient IDs
            
        Returns:
            List[Dict[str, Any]]: Reception task records
        """
        task_types = [
            'Appointment Confirmation',
            'Insurance Verification', 
            'Payment Processing',
            'Patient Check-in',
            'Reminder Call',
            'Follow-up'
        ]

        tasks = []

        # Generate tasks for recent period
        for _ in range(200):
            task_type = random.choice(task_types)

            # Some tasks are appointment-specific, others are patient-specific
            if task_type in ['Appointment Confirmation', 'Patient Check-in']:
                appointment_id = random.choice(appointment_ids)
                patient_id = None
            else:
                appointment_id = random.choice(appointment_ids) if random.random() < 0.7 else None
                patient_id = random.choice(patient_ids)

            # Task timing
            created_date = datetime.now() - timedelta(days=random.randint(1, 30))

            # Priority based on task type
            priority_map = {
                'Patient Check-in': 1,
                'Appointment Confirmation': 2,
                'Insurance Verification': 3,
                'Payment Processing': 2,
                'Reminder Call': 3,
                'Follow-up': 4
            }

            priority = priority_map.get(task_type, 3)

            # Task duration estimates
            duration_map = {
                'Appointment Confirmation': 3,
                'Insurance Verification': 8,
                'Payment Processing': 5,
                'Patient Check-in': 2,
                'Reminder Call': 4,
                'Follow-up': 6
            }

            estimated_duration = duration_map.get(task_type, 5)

            # Task status
            if created_date < datetime.now() - timedelta(days=1):
                status = random.choice(['Completed', 'Cancelled'])
                if status == 'Completed':
                    completed_date = created_date + timedelta(hours=random.randint(1, 48))
                    actual_duration = estimated_duration + random.randint(-2, 3)
                else:
                    completed_date = None
                    actual_duration = None
            else:
                status = random.choice(['Pending', 'In Progress'])
                completed_date = None
                actual_duration = None

            task = {
                'task_type': task_type,
                'patient_id': patient_id,
                'appointment_id': appointment_id,
                'priority': priority,
                'status': status,
                'assigned_to': random.choice(['Sarah', 'Mike', 'Jennifer', 'Auto-System']),
                'estimated_duration_minutes': estimated_duration,
                'actual_duration_minutes': actual_duration,
                'due_date': created_date + timedelta(hours=24),
                'completed_date': completed_date,
                'notes': self.fake.sentence() if random.random() < 0.3 else None,
                'created_at': created_date
            }

            tasks.append(task)

        return tasks

    def insert_data_to_database(self,
                            patients: List[Dict],
                            therapists: List[Dict],
                            appointments: List[Dict],
                            cancellations: List[Dict],
                            reception_tasks: List[Dict]) -> None:
        """
        Insert all generated data into the database.
        
        Args:
            patients (List[Dict]): Patient records
            therapists (List[Dict]): Therapist records
            appointments (List[Dict]): Appointment records
            cancellations (List[Dict]): Cancellation records
            reception_tasks (List[Dict]): Reception task records
        """
        cursor = self.conn.cursor()

        try:
            # Insert patients
            for patient in patients:
                cursor.execute('''
                    INSERT INTO patients 
                    (first_name, last_name, email, phone, date_of_birth, gender, address, 
                    insurance_type, primary_condition, registration_date, emergency_contact, 
                    emergency_phone, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    patient['first_name'], patient['last_name'], patient['email'],
                    patient['phone'], patient['date_of_birth'], patient['gender'],
                    patient['address'], patient['insurance_type'], patient['primary_condition'],
                    patient['registration_date'], patient['emergency_contact'],
                    patient['emergency_phone'], patient['notes']
                ))

            # Insert therapists
            for therapist in therapists:
                cursor.execute('''
                    INSERT INTO therapists 
                    (first_name, last_name, email, phone, specialization, license_number,
                    hire_date, hourly_rate, max_patients_per_day, working_days, 
                    start_time, end_time, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    therapist['first_name'], therapist['last_name'], therapist['email'],
                    therapist['phone'], therapist['specialization'], therapist['license_number'],
                    therapist['hire_date'], therapist['hourly_rate'], therapist['max_patients_per_day'],
                    therapist['working_days'], therapist['start_time'], therapist['end_time'],
                    therapist['is_active']
                ))

            # Insert appointments
            for apt in appointments:
                cursor.execute('''
                    INSERT INTO appointments 
                    (patient_id, therapist_id, treatment_id, appointment_date, appointment_time,
                    duration_minutes, status, booking_date, booking_method, price,
                    insurance_covered, copay_amount, notes, reminder_sent, check_in_time,
                    treatment_start_time, treatment_end_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    apt['patient_id'], apt['therapist_id'], apt['treatment_id'],
                    apt['appointment_date'], apt['appointment_time'], apt['duration_minutes'],
                    apt['status'], apt['booking_date'], apt['booking_method'], apt['price'],
                    apt['insurance_covered'], apt['copay_amount'], apt['notes'],
                    apt['reminder_sent'], apt.get('check_in_time'), 
                    apt.get('treatment_start_time'), apt.get('treatment_end_time')
                ))

            # Insert cancellations
            for cancel in cancellations:
                cursor.execute('''
                    INSERT INTO cancellations 
                    (appointment_id, cancelled_by, cancellation_date, hours_before_appointment,
                    reason_category, reason_detail, refund_issued, refund_amount, rescheduled)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    cancel['appointment_id'], cancel['cancelled_by'], cancel['cancellation_date'],
                    cancel['hours_before_appointment'], cancel['reason_category'],
                    cancel['reason_detail'], cancel['refund_issued'], cancel['refund_amount'],
                    cancel['rescheduled']
                ))

            # Insert reception tasks
            for task in reception_tasks:
                cursor.execute('''
                    INSERT INTO reception_tasks 
                    (task_type, patient_id, appointment_id, priority, status, assigned_to,
                    estimated_duration_minutes, actual_duration_minutes, due_date,
                    completed_date, notes, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    task['task_type'], task['patient_id'], task['appointment_id'],
                    task['priority'], task['status'], task['assigned_to'],
                    task['estimated_duration_minutes'], task['actual_duration_minutes'],
                    task['due_date'], task['completed_date'], task['notes'], task['created_at']
                ))

            self.conn.commit()
            print("[INFO] All data successfully inserted into database!")

        except Exception as e:
            self.conn.rollback()
            print(f"[INFO] Error inserting data: {e}")
            raise

    def generate_all_data(self) -> None:
        """
        Generate all clinic data and insert into database.
        
        This is the main method that orchestrates the entire data generation process.
        """
        print("[INFO] Starting clinic data generation...")

        # Generate patients
        print("[INFO] Generating patients...")
        patients = self.generate_patients(500)

        # Generate therapists 
        print("🧑[INFO] Generating therapists...")
        therapists = self.generate_therapists(6)

        # Get IDs after insertion to maintain referential integrity
        cursor = self.conn.cursor()

        # Insert patients and therapists first
        self.insert_data_to_database(patients, therapists, [], [], [])

        # Get patient and therapist IDs
        cursor.execute("SELECT patient_id FROM patients")
        patient_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT therapist_id FROM therapists")
        therapist_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT treatment_id FROM treatments")
        treatment_ids = [row[0] for row in cursor.fetchall()]

        # Generate appointments and cancellations
        print("[INFO] Generating appointments and cancellations...")
        appointments, cancellations = self.generate_appointments(
            patient_ids, therapist_ids, treatment_ids, 3000
        )

        # Generate reception tasks
        print("[INFO] Generating reception tasks...")
        cursor.execute("SELECT appointment_id FROM appointments")
        appointment_ids = [row[0] for row in cursor.fetchall()]
        reception_tasks = self.generate_reception_tasks(appointment_ids, patient_ids)

        # Insert remaining data
        print("[INFO] Inserting remaining data...")
        self.insert_data_to_database([], [], appointments, cancellations, reception_tasks)

        print("[INFO] Data generation complete!")
        self._print_summary_statistics()

    def _print_summary_statistics(self) -> None:
        """Print summary statistics of generated data."""
        cursor = self.conn.cursor()

        # Get counts
        cursor.execute("SELECT COUNT(*) FROM patients")
        patient_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM therapists")
        therapist_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM appointments")
        appointment_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM cancellations")
        cancellation_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM reception_tasks")
        task_count = cursor.fetchone()[0]

        # Calculate key metrics
        cancellation_rate = (cancellation_count / appointment_count) * 100

        cursor.execute("""
            SELECT AVG(price) FROM appointments WHERE status = 'Completed'
        """)
        avg_revenue_per_appointment = cursor.fetchone()[0]

        print(f"""CLINIC DATA SUMMARY
                =====================
                Patients: {patient_count:,}
                Therapists: {therapist_count}
                Appointments: {appointment_count:,}
                Cancellations: {cancellation_count:,} ({cancellation_rate:.1f}%)
                Reception Tasks: {task_count:,}
                Avg Revenue/Appointment: €{avg_revenue_per_appointment:.2f}
                Database ready for dashboard and ML model!""")


def main():
    """Main function to generate all clinic data."""


    # Initialize database with treatments
    print("[INFO] Setting up database...")
    db = ClinicDatabase()
    db.insert_sample_treatments()
    db.close()

    # Generate all data
    generator = ClinicDataGenerator()
    generator.generate_all_data()
    generator.conn.close()


if __name__ == "__main__":
    main()
