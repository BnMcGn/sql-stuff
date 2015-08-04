(in-package :sql-stuff)

(defmethod %fulltext-where 
    (text cols (database clsql-postgresql:postgresql-database))
  (let ((clauses
	 (collecting
	     (dolist (col (ensure-list cols))
	       (collect 
		   (sql-expression 
		    :string
		    (format nil "to_tsvector(~a) @@ to_tsquery('~a')"
			    (col-from-attribute-obj col)
			    (escape-sql-string text))))))))
    (list :where
	   (if (< 1 (length clauses)) 
		   (apply #'sql-or clauses)
		   (car clauses)))))

(defmethod %get-table-pkey (table 
			    (database clsql-postgresql:postgresql-database))
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

(defmethod %insert-record ((database clsql-postgresql:postgresql-database)
			   table values)
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