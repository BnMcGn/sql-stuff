;;;; sql-stuff.asd

(asdf:defsystem #:sql-stuff
  :serial t
  :description "Describe sql-stuff here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:clsql
               #:clsql-helper
;FIXME: Separate deps. needed for each database type.
               ;#:clsql-postgresql
               #:clsql-postgresql-socket3 ;See note in postgresql.lisp
               #:gadgets
               #:alexandria)
  :components ((:file "package")
               (:file "sql-stuff")
               (:file "postgresql")))


