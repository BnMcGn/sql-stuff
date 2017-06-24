(in-package :sql-stuff)

;;;FIXME: sql-stuff is currently only compatible with postgresql-socket3, not the
;;; plain driver. Defmethod can only specialize on one type at a time, so for now
;;; that is postgresql-socket3. Homework: find a way to get two types to
;;; dispatch to a single method. Don't want to write everything twice!

;;; clsql-postgresql:postgresql-database

(defmethod %fulltext-where (text cols (database (eql :postgresql)))
  (let ((clauses
   (collecting
       (dolist (col (ensure-list cols))
         (collect
       (sql-expression
        :string
        (format nil "to_tsvector(~a) @@ to_tsquery('~a')"
          (col-from-attribute-obj col)
          (sql-escape text))))))))
    (list :where
     (if (< 1 (length clauses))
       (apply #'sql-or clauses)
       (car clauses)))))

(defmethod %get-table-pkey (table (database (eql :postgresql)))
  (declare (ignore database))
    (with-a-database nil
      (grab-one
       (select
  (sql-expression :string "pg_catalog.pg_attribute.attname")
  :from (list
         (colm 'pg_catalog 'pg_attribute)
         (colm 'pg_catalog 'pg_constraint))
  :where
  (sql-and
   (sql-= (colm 'attrelid) (relation-oid-sql table))
   (sql-=
    (sql-expression :string "pg_catalog.pg_attribute.attnum")
    (sql-expression
     :string "pg_catalog.pg_constraint.conkey[1]"))
   (sql-=
    (sql-expression :string "pg_catalog.pg_constraint.contype") "p")
   (sql-=
    (sql-expression :string "pg_catalog.pg_constraint.conrelid")
    (relation-oid-sql table)))))))

(defmethod %insert-record ((database (eql :postgresql)) table values)
  (trycar
   'caar
   (with-a-database ()
     (clsql-sys:query
      (concatenate 'string
       (clsql-sys::sql-output
        (clsql-sys::make-sql-insert
         :into (sql-expression :table table)
         :av-pairs
         (mapcar (lambda (x)
             (list (intern (symbol-name (car x)))
             (if (equal (cdr x) "") nil (cdr x))))
           values)))
       (format nil
         " returning ~(~a~)"
         (get-table-pkey table)))))))

(defmethod %next-val ((database (eql :postgresql)) sequence)
  (car (query
  (strcat "select "
    (clsql-sys::sql-output (sql-function "nextval" sequence)))
  :flatp t)))

;;;FIXME: Whole package thing.

(defmethod %fulltext-where
    (text cols (database clsql-postgresql-socket3:postgresql-socket3-database))
  (%fulltext-where text cols :postgresql))

(defmethod %get-table-pkey
    (table
     (database clsql-postgresql-socket3:postgresql-socket3-database))
  (%get-table-pkey table :postgresql))

(defmethod %insert-record
    ((database clsql-postgresql-socket3:postgresql-socket3-database)
     table values)
  (%insert-record :postgresql table values))

(defmethod %next-val
    ((database clsql-postgresql-socket3:postgresql-socket3-database)
     sequence)
  (%next-val :postgresql sequence))



(defmethod %fulltext-where
    (text cols (database clsql-postgresql:postgresql-database))
  (%fulltext-where text cols :postgresql))

(defmethod %get-table-pkey
    (table
     (database clsql-postgresql:postgresql-database))
  (%get-table-pkey table :postgresql))

(defmethod %insert-record
    ((database clsql-postgresql:postgresql-database)
     table values)
  (%insert-record :postgresql table values))

(defmethod %next-val
    ((database clsql-postgresql:postgresql-database)
     sequence)
  (%next-val :postgresql sequence))
