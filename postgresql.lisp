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
    (take-one
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

(defmethod %get-table-columns ((database (eql :postgresql)) table)
  (declare (ignore database))
  (let ((res
         (with-a-database ()
              (select
               (sql-expression :string "a.attname")
               (sql-expression :string "pg_catalog.format_type(a.atttypid, a.atttypmod)")
               :from (sql-expression :string "pg_catalog.pg_attribute a")
               :where
               (sql-and
                (sql-> (colm 'a 'attnum) 0)
                (sql-not (colm 'a 'attisdropped))
                (sql-=
                 (colm 'a 'attrelid)
                 (sql-query
                  (colm 'c 'oid)
                  :from
                  (sql-expression
                   :string
                   "pg_catalog.pg_class c left join pg_catalog.pg_namespace n on n.oid = c.relnamespace")
                  :where (sql-and
                          (sql-expression
                           :string
                           (format nil "c.relname = '~a'"
                                   (to-lowercase (sql-escape (mkstr table)))))
                          (sql-expression :string
                                          "pg_catalog.pg_table_is_visible(c.oid)")))))))))
    (values-list (apply #'mapcar (lambda (&rest x) x) res))))

(defmethod %insert-record ((database (eql :postgresql)) table values)
  (trycar
   'caar
   (with-a-database ()
     (clsql-sys:query
      (concatenate 'string
                   (clsql-sys::sql-output
                    (clsql-sys::make-sql-insert
                     :into (tabl table)
                     :av-pairs
                     (mapcar (lambda (x)
                               (list (escape-error (car x))
                                     (if (equal (cdr x) "") nil (cdr x))))
                             values)))
                   (when-let ((pcol (get-table-pkey table)))
                     (format nil " returning ~(~a~)" pcol)))))))

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

(defmethod %get-table-columns
    ((database clsql-postgresql-socket3:postgresql-socket3-database)
     table)
  (%get-table-columns :postgresql table))


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

(defmethod %get-table-columns
    ((database clsql-postgresql:postgresql-database)
     table)
  (%get-table-columns :postgresql table))
