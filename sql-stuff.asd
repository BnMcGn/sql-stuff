;;;; sql-stuff.asd

(asdf:defsystem #:sql-stuff
  :serial t
  :description "Describe sql-stuff here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:clsql
               #:clsql-helper
               #:gadgets
               #:alexandria)
  :components ((:file "package")
               (:file "sql-stuff")))

