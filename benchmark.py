import os
import shutil
import time
import random

def main():
    db_dir = os.path.join(os.getcwd(), 'benchmarkdb')
    if os.path.exists(db_dir):
        print(f"Removing existing database directory: {db_dir}")
        shutil.rmtree(db_dir)

    from FileSystem import FileMgr
    from BufferPool import LogMgr, BufferMgr
    from Transaction import Transaction
    from Metadata import MetadataMgr
    from Planner import BasicQueryPlanner, BasicUpdatePlanner, Planner
    from Record import Schema, TableScan

    class BenchmarkDB:
        def __init__(self, db_name, block_size, buffer_pool_size):
            self.fm = FileMgr(db_name, block_size)
            self.lm = LogMgr(self.fm, db_name + '.log')
            self.bm = BufferMgr(self.fm, self.lm, buffer_pool_size)
    
            tx = Transaction(self.fm, self.lm, self.bm)
            if self.fm.db_exists:
                print('Recovering...')
                tx.recover()
                self.mm = MetadataMgr(tx, False)
            else:
                print('Created new db...')
                self.mm = MetadataMgr(tx, True)
            tx.commit()

    print("Initializing Database...")
    db = BenchmarkDB('benchmarkdb', 8192, 1000)
    tx = Transaction(db.fm, db.lm, db.bm)

    print("Creating schemas...")
    sStudent = Schema(['s_id', 'int', 4], ['s_name', 'str', 50], ['s_department', 'str', 30], ['s_year', 'int', 4])
    db.mm.createTable(tx, 'Student', sStudent)

    sInstructor = Schema(['i_id', 'int', 4], ['i_name', 'str', 50], ['i_department', 'str', 30])
    db.mm.createTable(tx, 'Instructor', sInstructor)

    sCourse = Schema(['c_id', 'int', 4], ['c_title', 'str', 100], ['c_department', 'str', 30], ['c_credits', 'int', 4])
    db.mm.createTable(tx, 'Course', sCourse)

    sSection = Schema(['sec_id', 'int', 4], ['sec_course_id', 'int', 4], ['sec_instructor_id', 'int', 4], ['sec_semester', 'str', 10], ['sec_year', 'int', 4])
    db.mm.createTable(tx, 'Section', sSection)

    sEnrollment = Schema(['e_id', 'int', 4], ['e_student_id', 'int', 4], ['e_section_id', 'int', 4], ['e_grade', 'str', 2])
    db.mm.createTable(tx, 'Enrollment', sEnrollment)

    random.seed(42)

    departments = ['CS', 'EE', 'ME', 'Math', 'Physics', 'Biology', 'Chemistry', 'English', 'History']
    semesters = ['Fall', 'Spring', 'Summer']
    grades = ['A', 'B', 'C', 'D', 'F', 'NC']

    print("Populating database...")
    
    print("Inserting Students (20)...")
    ts_student = TableScan(tx, 'Student', db.mm.getLayout(tx, 'Student'))
    for i in range(1, 21):
        ts_student.nextEmptyRecord()
        ts_student.setInt('s_id', i)
        ts_student.setString('s_name', f"Student_{i}")
        ts_student.setString('s_department', random.choice(departments))
        ts_student.setInt('s_year', random.randint(2018, 2024))
    ts_student.closeRecordPage()

    print("Inserting Instructors (10)...")
    ts_instructor = TableScan(tx, 'Instructor', db.mm.getLayout(tx, 'Instructor'))
    for i in range(1, 11):
        ts_instructor.nextEmptyRecord()
        ts_instructor.setInt('i_id', i)
        ts_instructor.setString('i_name', f"Instructor_{i}")
        ts_instructor.setString('i_department', random.choice(departments))
    ts_instructor.closeRecordPage()

    print("Inserting Courses (5)...")
    ts_course = TableScan(tx, 'Course', db.mm.getLayout(tx, 'Course'))
    for i in range(1, 6):
        ts_course.nextEmptyRecord()
        ts_course.setInt('c_id', i)
        ts_course.setString('c_title', f"Course_{i}")
        ts_course.setString('c_department', random.choice(departments))
        ts_course.setInt('c_credits', random.choice([3, 4]))
    ts_course.closeRecordPage()

    print("Inserting Sections (50)...")
    ts_section = TableScan(tx, 'Section', db.mm.getLayout(tx, 'Section'))
    for i in range(1, 51):
        ts_section.nextEmptyRecord()
        ts_section.setInt('sec_id', i)
        ts_section.setInt('sec_course_id', random.randint(1, 5))
        ts_section.setInt('sec_instructor_id', random.randint(1, 10))
        ts_section.setString('sec_semester', random.choice(semesters))
        ts_section.setInt('sec_year', random.choice([2023, 2024]))
    ts_section.closeRecordPage()

    print("Inserting Enrollments (100)...")
    ts_enrollment = TableScan(tx, 'Enrollment', db.mm.getLayout(tx, 'Enrollment'))
    for i in range(1, 101):
        ts_enrollment.nextEmptyRecord()
        ts_enrollment.setInt('e_id', i)
        ts_enrollment.setInt('e_student_id', random.randint(1, 20))
        ts_enrollment.setInt('e_section_id', random.randint(1, 50))
        ts_enrollment.setString('e_grade', random.choice(grades))
    ts_enrollment.closeRecordPage()

    tx.commit()
    print("Database populated successfully!\n")

    """
    Q1. Students enrolled in CS courses (Slow join)
    Q2. Courses with high enrollment
    Q3. Students who received an NC grade
    Q4. Instructor teaching load per semester
    Q5. Filter by s_department
    """
    tx2 = Transaction(db.fm, db.lm, db.bm)
    qp = BasicQueryPlanner(db.mm)
    up = BasicUpdatePlanner(db.mm)
    p = Planner(qp, up)

    queries = [
        ("Q1", "select s_id, s_name from Student, Enrollment, Section, Course where s_id = e_student_id and e_section_id = sec_id and sec_course_id = c_id and c_department = 'CS'"),
        ("Q2", "select c_id, c_title from Course, Section, Enrollment where c_id = sec_course_id and sec_id = e_section_id and c_id = 50"),
        ("Q3", "select s_id, s_name from Student, Enrollment where s_id = e_student_id and e_grade = 'NC'"),
        ("Q4", "select i_id, i_name from Instructor, Section where i_id = sec_instructor_id and sec_semester = 'Fall' and sec_year = 2024"),
        ("Q5", "select s_department, e_grade from Student, Enrollment where s_id = e_student_id and s_department = 'CS'")
    ]

    print("=================== Executing Benchmark Queries ===================")
    for name, q in queries:
        print(f"\nExecuting {name}...")
        print(f"Query: {q}")
        start_time = time.time()
        
        try:
            plan = p.createQueryPlan(tx2, q)
            scan = plan.open()
            count = 0
            while scan.nextRecord():
                count += 1
            scan.closeRecordPage()
            
            end_time = time.time()
            print(f"--> {name} Results: found {count} rows in {end_time - start_time:.4f} seconds")
        except Exception as e:
            end_time = time.time()
            print(f"--> {name} Failed with error: {e}")
            print(f"Time elapsed before error: {end_time - start_time:.4f} seconds")
            
    tx2.commit()

if __name__ == "__main__":
    main()
