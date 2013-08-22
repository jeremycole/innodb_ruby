require "innodb/record_describer"

# CREATE TABLE hello_world (
#   id          INT             NOT NULL,
#   message     VARCHAR(100)    NOT NULL,
#   author      VARCHAR(100)    NOT NULL,
#   PRIMARY KEY (id),
#   KEY message (message)
# ) ENGINE=InnoDB;
#
# INSERT INTO hello_world (id, message, author) VALUES (1, "Hello", "Jack");
# INSERT INTO hello_world (id, message, author) VALUES (2, "World", "Jill");

class HelloWorld_PRIMARY < Innodb::RecordDescriber
  type :clustered
  key "id",             :INT, :NOT_NULL
  row "message",        "VARCHAR(100)", :NOT_NULL
  row "author",         "VARCHAR(100)", :NOT_NULL
end

class HelloWorld_message < Innodb::RecordDescriber
  type :secondary
  key "message",        "VARCHAR(100)", :NOT_NULL
  row "id",             :INT, :NOT_NULL
end
