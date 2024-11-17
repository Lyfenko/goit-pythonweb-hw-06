from sqlalchemy import func, desc, select, and_

from database.models import Teacher, Student, Discipline, Grade, Group
from database.db import session


def select_one():
    result = (
        session.query(
            Student.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade")
        )
        .select_from(Grade)
        .join(Student)
        .group_by(Student.id)
        .order_by(desc("avg_grade"))
        .limit(5)
        .all()
    )
    return result


def select_two():
    result = (
        session.query(
            Discipline.name,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("avg_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .filter(Discipline.id == 5)
        .group_by(Student.id, Discipline.name)
        .order_by(desc("avg_grade"))
        .limit(1)
        .first()
    )
    return result


def select_3(discipline_name):
    results = (
        session.query(Group.name, func.avg(Grade.grade).label("avg_grade"))
        .select_from(Grade)
        .join(Student)
        .join(Group)
        .join(Discipline)
        .filter(Discipline.name == discipline_name)
        .group_by(Group.id)
        .all()
    )
    return results


def select_4():
    results = session.query(func.avg(Grade.grade).label("avg_grade")).first()
    return results


def select_5(teacher_name):
    results = (
        session.query(Discipline.name)
        .join(Teacher)
        .filter(Teacher.fullname == teacher_name)
        .all()
    )
    return results


def select_6(group_name):
    results = (
        session.query(Student.fullname)
        .join(Group)
        .filter(Group.name == group_name)
        .all()
    )
    return results


def select_7(group_name, discipline_name):
    results = (
        session.query(Student.fullname, Grade.grade)
        .join(Group)
        .join(Grade)
        .join(Discipline)
        .filter(Group.name == group_name)
        .filter(Discipline.name == discipline_name)
        .all()
    )
    return results


def select_8(teacher_name: str):
    result = (
        session.query(
            func.avg(Grade.grade).label("avg_grade"),
        )
        .join(Discipline)
        .join(Teacher)
        .filter(Teacher.fullname == teacher_name)
        .group_by(Teacher.fullname, Discipline.name)
        .all()
    )
    return result


def select_9(student_name: str):
    result = (
        session.query(
            Discipline.name,
        )
        .join(Grade)
        .join(Student)
        .filter(Student.fullname == student_name)
        .group_by(Discipline.name)
        .all()
    )
    return result


def select_10(student_name: str, teacher_name: str):
    result = (
        session.query(
            Discipline.name,
        )
        .join(Grade)
        .join(Student)
        .join(Discipline.teacher)
        .filter(
            and_(
                Student.fullname == student_name,
                Teacher.fullname == teacher_name,
            )
        )
        .group_by(Discipline.name)
        .all()
    )
    return result


def select_11(teacher_fullname: str, student_fullname: str) -> float:
    result = (
        session.query(func.avg(Grade.grade))
        .join(Student)
        .join(Discipline)
        .join(Teacher)
        .filter(Teacher.fullname == teacher_fullname)
        .filter(Student.fullname == student_fullname)
        .first()[0]
    )

    return result


def select_12():
    subquery = (
        select(func.max(Grade.date_of))
        .join(Student)
        .filter(and_(Grade.discipline_id == 3, Student.group_id == 3))
        .scalar_subquery()
    )

    result = (
        session.query(Student.id, Student.fullname, Grade.grade, Grade.date_of)
        .select_from(Grade)
        .join(Student)
        .filter(
            and_(
                Grade.discipline_id == 3,
                Student.group_id == 3,
                Grade.date_of == subquery,
            )
        )
        .all()
    )
    return result


if __name__ == "__main__":
    print(select_one())
    print(select_two())
    print(select_3("Mathematics for Computer Science"))
    print(select_4())
    print(select_5("Volodymyr Vovtenko"))
    print(select_6("АА5"))
    print(select_7("PP33", "Design and Analysis of Algorithms"))
    print(select_8("Prymak Alena"))
    print(select_9("Lyfenko Dmytro"))
    print(select_10("Milolika Lyka", "Diane Burke"))
    print(select_11("Gregory Robertson", "Milentiy Lyka"))
    print(select_12())