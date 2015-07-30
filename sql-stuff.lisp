;;;; sql-stuff.lisp

(in-package #:sql-stuff)

;;; "sql-stuff" goes here. Hacks and glory await!

(eval-when (:compile-toplevel :load-toplevel :execute)
  (disable-sql-reader-syntax) ;do a full reset
  (enable-sql-reader-syntax))

(defun colm (table-or-column &optional (column nil))
  (if column
      (sql-expression 
	:table table-or-column 
	:attribute column)
      (sql-expression :attribute table-or-column)))

(defun tabl (table)
  (sql-expression :table table))

(defun escape-sql-string (str)
  (format nil "~{~a~^''~}" (split-sequence #\' str)))

(defun joinspec->whereclause (jspec)
  (join-bind jspec
    `(sql-and 
      (sql-= (colm ',table1 ',pkey1) (colm ',join-table ',fkey1))
      (sql-= (colm ',table2 ',pkey2) (colm ',join-table ',fkey2)))))

;joinspec format: table1 pkey1 jointable-fkey1 jointable jointable-fkey2 pkey2 table2
;this is mirror image: can be flipped for other direction
;example:
;(defvar *bookauthor-join*
;  '(books bookid bookid bookauthors authorid authorid authors))

(defmacro join-bind (jspec &body body)
  `(destructuring-bind (table1 pkey1 fkey1 join-table fkey2 pkey2 table2) ,jspec
     (declare (ignorable table1 pkey1 fkey1 join-table fkey2 pkey2 table2))
     ,@body))

(defun and-merge-where-clauses (&rest clauses)
  (let ((items (collecting 
	  (dolist (clause clauses)
	    (if (eq (car clause) 'sql-and)
		(dolist (subclause (cdr clause))
		  (collect subclause))
		(when clause
		  (collect clause)))))))
    (if (< 1 (length items))
	(cons 'sql-and items)
	items)))

(defun join-add (select joinspec)
  (join-bind joinspec
    (let 
	((tables (keyword-value :from select))
	 (select (copy-list select)))
      (setf 
       (keyword-value :from select)
       (if (atom tables)
	   (list tables join-table)
	   `(list (sql-expression :table ',join-table) ,@tables)))
      (setf
       (keyword-value :where select)
       (and-merge-where-clauses
	(keyword-value :where select) (joinspec->whereclause joinspec)))
      select)))

(defun query-components (&rest queries)
  (with-collectors (cols< from< where< other<)
    (dolist (query queries)
      (multiple-value-bind (cols specs) 
	  (divide-list query #'keywordp)
	(mapc #'cols< (cdr cols))
	(dolist (clause (keyword-splitter specs))
	  (case (car clause)
	    (:from
	     (mapc #'from< (ensure-list (cdr clause))))
	    (:where
	     (where< (cdr clause)))
	    (otherwise
	     (other< (car clause))
	     (other< (cdr clause)))))))))

(defparameter *execute-query* t)

(defun query-code-p (thing)
  "Does thing - presumably an s-expression - represent a call to a query 
   function?"
  (when (listp thing)
    (member (car thing) '(select delete update))))

(defun %%unexecute-query (code)
  (if (query-code-p code)
      (list* 'list `(quote ,(car code)) (cdr code))
      code))

(defmacro unexecute (&body code)
  `(let ((*execute-query* nil))
     ,@(%%unexecute-query code)))

(defun %%merge-query (&rest queries)
  (multiple-value-bind (cols from where other)
      (apply #'query-components queries)
    `(,@(when (query-code-p (car queries))
	      (list (caar queries)))
	,@cols 
       ,@(when from 
	       (list :from (if (< 1 (length from)) from (car from))))
       ,@(when where 
	       (list :where (apply #'sql-and where))) ,@other)))
 
;Defined as macro to keep clsql queries from executing immediately
(defmacro merge-query (&rest queries)
  "Query fragments must start with a keyword."
  `(let ((q (let ((*execute-query* nil))
	      (apply #'%%merge-query 
		     ,(cons 'list (mapcar #'%%unexecute-query queries))))))
     (if (and *execute-query* (query-code-p q))
	 (apply-car q)
	 q)))

(defmacro def-query (name (&rest lambda-list) &body body)
  (with-gensyms (query)
    (multiple-value-bind (wrapcode qcode overflow)
	(tree-search-replace 
	 body
	 :key (lambda (x) (trycar 'car x)) :match 'query-marker
	 :valuefunc (lambda (x) (if (query-code-p x)
				    `(apply-car ,query)
				    query)))
      (when overflow
	(error "Only one query-marker allowed!"))
      (if (null qcode)
	(progn (setf qcode wrapcode) 
	       (setf wrapcode nil))
	(setf qcode (cadr qcode)))
    `(defun ,name ,lambda-list
       (let ((,query ,(%%unexecute-query qcode)))
	 (if *execute-query*
	     ,(if wrapcode
		  (list* 'progn wrapcode)
		  (if (query-code-p qcode)
		      `(apply-car ,query)
		      query))
	     ,query))))))

(defun add-count (query)
  (multiple-value-bind (cols mods) (divide-list query #'keywordp)
    `(,(car cols)
       ,(apply #'sql-count (cdr cols))
       ,@mods)))

(defun get-count (query)
  (with-a-database ()
    (grab-one
     (apply-car
      (add-count query)))))
  
;FIXME - only finds select at caar. May need treewalker.
(defmacro with-m2m (joinspec select)
  `(,@(join-add select (symbol-value joinspec))))

(defun add-limit (limit offset query)
  (declare (type (or null integer) limit)
	   (type (or null integer) offset))
  (if (or limit offset)
      (merge-query query
		   `(,@(when limit (list :limit limit))
		       ,@(when offset (list :offset offset))))
      query))

(defun limit-mixin (limit offset)
  (declare (type (or null integer) limit)
	   (type (or null integer) offset))
  `(,@(when limit (list :limit limit))
    ,@(when offset (list :offset offset))))

(defun order-by-mixin (orderspec)
  (when orderspec
    `(:order-by ,orderspec)))

(defun add-order-by (orderspec query)
  (if orderspec
    (merge-query query `(:order-by ,orderspec))
    query))

(defun apply-car (data)
  (apply (symbol-function (car data)) (cdr data)))

(defmacro mod-query ((&rest additions) &body query)
  "Keyword :execute, true by default, indicates if the modified query should be executed. To get the code, set it to nil."
  (labels ((proc (clauses code)
	     (if clauses
		 `(,@(car clauses) ,(proc (cdr clauses) code))
		 code)))
    `,(proc additions (if (member (caar query) '(select delete update))
			 `(list ',(caar query) ,@(cdar query))
			 (car query)))))

(defmacro quick-mod-query (&body query)
  `(mod-query 
       ((add-limit limit offset)
	(add-order-by order-by))
     ,@query))

(defun in-or-equal (col key/s)
  (if (listp key/s)
      (sql-in col key/s)
      (sql-= col key/s)))

(defun table-from-attribute-obj (attobj)
  (case (type-of attobj)
    (clsql-sys:sql-ident-table (slot-value attobj 'clsql-sys:name))
    (clsql-sys:sql-ident-attribute (slot-value attobj 'clsql-sys::qualifier))
    (otherwise (error "Can't get table name"))))

(defun col-from-attribute-obj (attobj)
  (assert (eq (type-of attobj) 'clsql-sys:sql-ident-attribute))
  (slot-value attobj 'clsql-sys:name))

(defun col-from-pkey-query (col key/s &key limit offset order-by)
  (with-a-database ()
    (quick-mod-query
      (select
       col
       :from (table-from-attribute-obj col)
       :where (in-or-equal 
	       (symbolize (get-table-pkey (table-from-attribute-obj col)))
	       key/s)))))

(defun col-from-pkey (col key/s &key limit offset order-by)
  "Given a column with table attribute, (see colm), will extrapolate the pkey column, returning records of col where pkey is one of key/s. Key/s can be a single key or a list of keys."
  (with-a-database ()
    (mapcar #'car
	    (apply-car (col-from-pkey-query
			col key/s :limit limit :offset offset 
			:order-by order-by)))))

(defun get-pkeys-for-pkey-query (joinspec pkey &key limit offset order-by)
  "Gets foreign pkeys that are tied to a pkey across a many to many relation. Joinspec will tell which direction the relation is pointing"
  (join-bind joinspec
    (quick-mod-query 
      (select 
       (colm join-table fkey2) 
       :from (tabl join-table)
       :where (in-or-equal (colm join-table fkey1) pkey)
       :distinct t))))

(defun get-pkeys-for-pkey (joinspec pkey &key limit offset order-by)
  "Gets foreign pkeys that are tied to a pkey across a many to many relation. Joinspec will tell which direction the relation is pointing"
  (with-a-database ()
    (mapcar 
     #'car
     (apply-car
      (get-pkeys-for-pkey-query 
       joinspec pkey 
       :limit limit :offset offset :order-by order-by)))))

(defun update-pkeys-for-pkey (joinspec pkey pkeys)
  (with-a-database ()
    (join-bind joinspec
      (let* ((pkeys (mapcar #'string-unless-number pkeys))
	     (oldpkeys (get-pkeys-for-pkey joinspec pkey))
	     (newkeys (set-difference pkeys oldpkeys :test #'equal))
	     (remkeys (set-difference oldpkeys pkeys :test #'equal)))
	(dolist (k newkeys)
	  (insert-records 
	   :into (tabl join-table)
	   :attributes (list (colm fkey1) (colm fkey2))
	   :values (list pkey k)))
	(dolist (k2 remkeys)
	  (delete-records 
	   :from (tabl join-table)
	   :where (sql-and 
		   (sql-= (colm fkey1) pkey)
		   (sql-= (colm fkey2) k2))))))))

(defun get-pkeys-for-pkey/chain-query (joinspecs pkey 
				       &key limit offset order-by)
  (let* ((current (car joinspecs))
	 (final (null (cdr joinspecs)))
	 (climit (and final limit))
	 (coffset (and final offset))
	 (corder-by (and final order-by))
	 (res
	  (cond
	    ((eq (type-of current) 'clsql-sys:sql-ident-attribute)
	     (mod-query
		 ((add-limit climit coffset)
		  (add-order-by corder-by)
	       (select 
		(colm (get-table-pkey (table-from-attribute-obj current)))
		:from (table-from-attribute-obj current)
		:where (sql-in current (col-from-pkey current pkey))
		:distinct t))))
	    ((= 7 (length current)) 
	     (get-pkeys-for-pkey-query current pkey
	       :limit climit :offset coffset :order-by corder-by))
	    ((= 2 (length current)) 
	     (if (eq (type-of (car current)) 'clsql-sys:sql-ident-table)
		 (mod-query
		     ((add-limit climit coffset)
		      (add-order-by corder-by)
		   (select (sql-expression 
			    :attribute 
			    (get-table-pkey (table-from-attribute-obj 
					     (second current))))
			   :from (sql-expression 
				  :table 
				  (table-from-attribute-obj (second current)))
			   :where (in-or-equal (second current) pkey)
			   :distinct t))
		 (col-from-pkey-query (car current) pkey))))
	    (t (error "Shouldn't be here")))))
    (if (not final)
	(get-pkeys-for-pkey/chain (cdr joinspecs) (apply-car res) 
	  :limit limit :offset offset :order-by order-by)
	res)))

(defun get-pkeys-for-pkey/chain (joinspecs pkey &key limit offset order-by)
  (with-a-database ()
    (apply-car
     (get-pkeys-for-pkey/chain-query 
      joinspecs pkey 
      :limit limit :offset offset :order-by order-by))))

(defun get-record-by-pkey (table id)
  (with-a-database ()
    (multiple-value-passthru (data labels) 
	(select (sql-expression :attribute '*) 
		:from (sql-expression :table table)
		:where (sql-= 
			(sql-expression :attribute (get-table-pkey table)) 
			id))
      (car data) labels)))

(defun get-assoc-by-pkey (table id)
  (apply #'pairlis
	 (multiple-value-bind (a b)
		    (get-record-by-pkey table id)
		  (list (mapcar (lambda (x) 
				  (symbolize x :package 'keyword))
				b) a))))

(defun update-record (table pkey values)
  (with-a-database ()
    (update-records 
     (sql-expression :table table)
     :av-pairs 
     (mapcar (lambda (x)
	       (list (intern (symbol-name (car x)))
		     (if (equal (cdr x) "") nil (cdr x))))
	     values)
     :where (sql-= (sql-expression :attribute (get-table-pkey table)) pkey))))
 
(defun postgres-insert-id (table)
  (values
   (trycar 
    'caar
    (clsql-sys:query 
     (format 
      nil 
      "SELECT CURRVAL(pg_get_serial_sequence(~('~a','~a'~)));" 
      table (get-table-pkey table))))))

;;FIXME: only works with postgres. genericfunc?
(defun insert-record (table values)
  (values
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
			    (get-table-pkey table))))))))

(defun insert-or-update (table key data)
  (print data)
  (if key
      (progn
	(update-record table key data)
	key)
      (insert-record table data)))

(defun get-column-query (table col &key limit offset order-by)
  (quick-mod-query
    (select 
     (sql-expression :attribute col)
     :from
     (sql-expression :table table)
     :distinct t)))

(defun get-column (table col &key limit offset order-by)
  (with-a-database ()
    (mapcar #'car 
	    (apply-car
	     (get-column-query 
	      table col
	      :limit limit :offset offset :order-by order-by)))))

(defun get-columns-query (table &rest cols)
  (multiple-value-bind (keys cols)
      (extract-keywords '(:limit :offset :order-by) cols)
    (let ((cols (loop for c in cols
		   collect (colm c)))
	  (limit (assoc-cdr :limit keys))
	  (offset (assoc-cdr :offset keys))
	  (order-by (assoc-cdr :order-by keys)))
      (quick-mod-query 
	`(select  
	  ,@cols
	  :from
	  ,(tabl table)
	  :distinct t)))))

(defun get-columns (table &rest cols)
  (with-a-database ()
    (apply-car 
     (apply #'get-columns-query table cols))))

(defun grab-one (query)
  (trycar 'caar query))

;;; borrowed from clsql-pg-introspect
(defmacro ensure-strings ((&rest vars) &body body)
  `(let ,(loop for var in vars
	       collect `(,var (if (stringp ,var)
				  ,var
				  (symbol-name ,var))))
    ,@body))

(defun relation-oid-sql (table)
  (declare (type (or symbol string) table))
  (ensure-strings (table)
    (sql-expression :string 
		    (format nil "'~A'::regclass" (normalize-for-sql table)))))

(defun normalize-for-sql (string)
  (substitute #\_ #\- string))
;END borrowed

(defun get-table-pkey (table)
  (%get-table-pkey table *default-database*))

(defgeneric %get-table-pkey (table database))
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

(defgeneric %fulltext-where (text cols database)
  (:documentation "Creates the where clause for a fulltext search of cols."))

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

(def-query fulltext-search (text cols &key limit offset order-by)
  "Warning: doesn't create indices in database. Do so for more speed."
  (mapcar 
   #'car
   (query-marker
    (let ((table (dolist (col (ensure-list cols))
		   (awhen (table-from-attribute-obj col)
		     (return it)))))
      (assert table)
      (merge-query
       (select 
	(colm (get-table-pkey table))
	:from (tabl table))
       (%fulltext-where text cols *default-database*)
       (limit-mixin limit offset)
       (order-by-mixin order-by))))))

