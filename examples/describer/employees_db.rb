require "innodb/record_describer"

# CREATE TABLE employees (
#     emp_no      INT             NOT NULL,
#     birth_date  DATE            NOT NULL,
#     first_name  VARCHAR(14)     NOT NULL,
#     last_name   VARCHAR(16)     NOT NULL,
#     gender      ENUM ('M','F')  NOT NULL,
#     hire_date   DATE            NOT NULL,
#     PRIMARY KEY (emp_no)
# );

class Employees_employees_PRIMARY < Innodb::RecordDescriber
  type :clustered
  key "emp_no",         :INT, :NOT_NULL
  row "birth_date",     :MEDIUMINT, :NOT_NULL
  row "first_name",     "VARCHAR(14)", :NOT_NULL
  row "last_name",      "VARCHAR(16)", :NOT_NULL
  row "gender",         :TINYINT, :UNSIGNED, :NOT_NULL
  row "hire_date",      :MEDIUMINT, :NOT_NULL
end

# CREATE TABLE departments (
#     dept_no     CHAR(4)         NOT NULL,
#     dept_name   VARCHAR(40)     NOT NULL,
#     PRIMARY KEY (dept_no),
#     UNIQUE  KEY (dept_name)
# );

class Employees_departments_PRIMARY < Innodb::RecordDescriber
  type :clustered
  key "dept_no",        "CHAR(4)", :NOT_NULL
  row "dept_name",      "VARCHAR(40)", :NOT_NULL
end

class Employees_departments_dept_name < Innodb::RecordDescriber
  type :secondary
  key "dept_name",      "VARCHAR(40)", :NOT_NULL
  row "dept_no",        "CHAR(4)", :NOT_NULL
end

# CREATE TABLE dept_manager (
#    dept_no      CHAR(4)         NOT NULL,
#    emp_no       INT             NOT NULL,
#    from_date    DATE            NOT NULL,
#    to_date      DATE            NOT NULL,
#    KEY         (emp_no),
#    KEY         (dept_no),
#    FOREIGN KEY (emp_no)  REFERENCES employees (emp_no)    ON DELETE CASCADE,
#    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
#    PRIMARY KEY (emp_no,dept_no)
# );

class Employees_dept_manager_PRIMARY < Innodb::RecordDescriber
  type :clustered
  key "emp_no",         :INT, :NOT_NULL
  key "dept_no",        "CHAR(4)", :NOT_NULL
  row "from_date",      :MEDIUMINT, :NOT_NULL
  row "to_date",        :MEDIUMINT, :NOT_NULL
end

class Employees_dept_manager_emp_no < Innodb::RecordDescriber
  type :secondary
  key "emp_no",         :INT, :NOT_NULL
  row "dept_no",        "CHAR(4)", :NOT_NULL
end

class Employees_dept_manager_dept_no < Innodb::RecordDescriber
  type :secondary
  key "dept_no",        "CHAR(4)", :NOT_NULL
  row "emp_no",         :INT, :NOT_NULL
end

# CREATE TABLE dept_emp (
#     emp_no      INT             NOT NULL,
#     dept_no     CHAR(4)         NOT NULL,
#     from_date   DATE            NOT NULL,
#     to_date     DATE            NOT NULL,
#     KEY         (emp_no),
#     KEY         (dept_no),
#     FOREIGN KEY (emp_no)  REFERENCES employees   (emp_no)  ON DELETE CASCADE,
#     FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
#     PRIMARY KEY (emp_no,dept_no)
# );

class Employees_dept_emp_PRIMARY < Innodb::RecordDescriber
  type :clustered
  key "emp_no",         :INT, :NOT_NULL
  key "dept_no",        "CHAR(4)", :NOT_NULL
  row "from_date",      :MEDIUMINT, :NOT_NULL
  row "to_date",        :MEDIUMINT, :NOT_NULL
end

class Employees_dept_emp_emp_no < Innodb::RecordDescriber
  type :secondary
  key "emp_no",         :INT, :NOT_NULL
  row "dept_no",        "CHAR(4)", :NOT_NULL
end

class Employees_dept_emp_dept_no < Innodb::RecordDescriber
  type :secondary
  key "dept_no",        "CHAR(4)", :NOT_NULL
  row "emp_no",         :INT, :NOT_NULL
end

# CREATE TABLE titles (
#     emp_no      INT             NOT NULL,
#     title       VARCHAR(50)     NOT NULL,
#     from_date   DATE            NOT NULL,
#     to_date     DATE,
#     KEY         (emp_no),
#     FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
#     PRIMARY KEY (emp_no,title, from_date)
# );

class Employees_titles_PRIMARY < Innodb::RecordDescriber
  type :clustered
  key "emp_no",         :INT, :NOT_NULL
  key "title",          "VARCHAR(50)", :NOT_NULL
  key "from_date",      :MEDIUMINT, :NOT_NULL
  row "to_date",        :MEDIUMINT, :NOT_NULL
end

class Employees_titles_emp_no < Innodb::RecordDescriber
  type :secondary
  key "emp_no",         :INT, :NOT_NULL
  row "title",          "VARCHAR(50)", :NOT_NULL
  row "from_date",      :MEDIUMINT, :NOT_NULL
end

# CREATE TABLE salaries (
#     emp_no      INT             NOT NULL,
#     salary      INT             NOT NULL,
#     from_date   DATE            NOT NULL,
#     to_date     DATE            NOT NULL,
#     KEY         (emp_no),
#     FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
#     PRIMARY KEY (emp_no, from_date)
# );

class Employees_salaries_PRIMARY < Innodb::RecordDescriber
  type :clustered
  key "emp_no",         :INT, :NOT_NULL
  key "from_date",      :MEDIUMINT, :NOT_NULL
  row "salary",         :INT, :NOT_NULL
  row "to_date",        :MEDIUMINT, :NOT_NULL
end

class Employees_salaries_emp_no < Innodb::RecordDescriber
  type :secondary
  key "emp_no",         :INT, :NOT_NULL
  row "from_date",      :MEDIUMINT, :NOT_NULL
end
