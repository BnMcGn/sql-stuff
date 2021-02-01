;;;; sql-stuff.lisp

(in-package #:sql-stuff)

;;; "sql-stuff" goes here. Hacks and glory await!

(eval-always
  (disable-sql-reader-syntax) ;do a full reset
  (enable-sql-reader-syntax))

(eval-always
  (defgeneric sql-escape (item))
  (defmethod sql-escape ((item string))
    (format nil "~{~a~^''~}" (cl-utilities:split-sequence #\' item)))
  (defmethod sql-escape ((item number))
    item)
  (defmethod sql-escape ((item t))
    item)

  (defun escape-error (item)
    (if (find #\' (mkstr item))
        (error "Possible SQL injection attempt! Strange single quote observed.")
        item))

  (defun macro-checkable-p (itm)
    "Determine if the macro can safety check itm at compile time"
    (or (not itm)
        (stringp itm)
        (keywordp itm)
        (and (consp itm)
             (eq 'quote (car itm))
             (symbolp (second itm)))))

  (defun check-sql-form (form)
    (if (macro-checkable-p form)
        (if (consp form)
            (progn
              (escape-error (second form))
              form)
            (escape-error form))
        `(escape-error ,form))))

(defun %colm (table-or-column column)
  (if column
      (sql-expression
       :table table-or-column
       :attribute column)
      (sql-expression :attribute table-or-column)))

(defmacro colm (table-or-column &optional (column nil))
  `(%colm ,(check-sql-form table-or-column) ,(check-sql-form column)))

(defmacro colms (&rest columns)
  `(list ,@(mapcar (lambda (col) `(%colm ,(check-sql-form col) nil)) columns)))

(defun %tabl (table)
  (sql-expression :table table))

(defmacro tabl (table)
  `(%tabl ,(check-sql-form table)))

(defgeneric table-symbol (table-repr)
  (:documentation "Coerces table representation into a symbol")
  (:method ((table-repr symbol)) table-repr)
  (:method ((table-repr string)) (intern table-repr 'keyword))
  (:method ((table-repr clsql-sys:sql-ident-attribute))
    (slot-value table-repr 'clsql-sys::qualifier))
  (:method ((table-repr clsql-sys:sql-ident-table))
    (slot-value table-repr 'clsql-sys::name)))

;;joinspec format: table1 pkey1 jointable-fkey1 jointable jointable-fkey2 pkey2 table2
;;this is mirror image: can be flipped for other direction
;;example:
;;(defvar *bookauthor-join*
;;  '(books bookid bookid bookauthors authorid authorid authors))

(defmacro join-bind (jspec &body body)
  `(destructuring-bind (table1 pkey1 fkey1 join-table fkey2 pkey2 table2) ,jspec
     (declare (ignorable table1 pkey1 fkey1 join-table fkey2 pkey2 table2))
     ,@body))

(defun m2m-mixin (joinspec)
  (join-bind joinspec
    `(:where
      (sql-and
       (sql-= (colm ',table1 ',pkey1) (colm ',join-table ',fkey1))
       (sql-= (colm ',table2 ',pkey2) (colm ',join-table ',fkey2))))))

(defun query-components (&rest queries)
  (cl-utilities:with-collectors (cols< from< where< other<)
    (dolist (query queries)
      (multiple-value-bind (cols specs)
          (part-on-true #'keywordp query)
        (mapc #'cols< (cdr cols))
        (dolist (clause (proto:keyword-splitter specs))
          (case (car clause)
            (:from
             (mapc #'from< (ensure-list (cdr clause))))
            (:where
             (where< (cdr clause)))
            (otherwise
             (other< (car clause))
             (other< (cdr clause)))))))))

(defparameter *execute-query* t)

(eval-always
  (defun query-code-p (thing)
    "Does thing - presumably an s-expression - represent a call to a query
   function?"
    (when (listp thing)
      (member (car thing) '(select delete update))))

  (defun %%unexecute-query (code)
    (if (query-code-p code)
        (list* 'list `(quote ,(car code)) (cdr code))
        code)))

(defmacro unexecuted (&body code)
  `(let ((*execute-query* nil))
     ,@(mapcar #'%%unexecute-query code)))

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
  ;;FIXME: this doesn't work, but better checking would be nice here
  ;;(assert (every #'listp queries))
  `(let ((q (let ((*execute-query* nil))
              (apply #'%%merge-query
                     ,(cons 'list (mapcar #'%%unexecute-query queries))))))
     (if (and *execute-query* (query-code-p q))
         (apply-car q)
         q)))

(defmacro def-query (name (&rest lambda-list) &body body)
  (with-gensyms (query)
    (multiple-value-bind (wrapcode qcode overflow)
        (proto:tree-search-replace
         body
         :key (lambda (x) (trycar 'car x)) :match 'query-marker
         :value `(apply-car ,query))
      (when overflow
        (error "Only one query-marker allowed!"))
      (if (null qcode)
          (progn (setf qcode (car wrapcode))
                 (setf wrapcode nil))
          (setf qcode (cadr qcode)))
      `(defun ,name ,lambda-list
         (let ((,query
                (let ((*execute-query* nil))
                  ,(%%unexecute-query qcode))))
           (if *execute-query*
               ,(if wrapcode
                    `(progn ,@wrapcode)
                    `(apply-car ,query))
               ,query))))))

(defun add-count (query)
  (multiple-value-bind (cols mods) (part-on-true #'keywordp query)
    `(,(car cols)
       ,(sql-count (if (listp (second cols)) (car (second cols)) (second cols)))
      ,@(nth-value 1 (extract-keywords '(:order-by) mods)))))

(defun get-count (query)
  (with-a-database ()
    (take-one
     (apply-car
      (add-count query)))))

(defun exists (query)
  (unless query
    (error "Query can't be NIL"))
  (< 0 (get-count query)))

(defun limit-mixin (limit offset)
  (declare (type (or null integer) limit)
           (type (or null integer) offset))
  `(,@(when limit (list :limit (sql-escape limit)))
      ,@(when offset (list :offset (sql-escape offset)))))

(defun order-by-mixin (&rest orderspecs)
  (when (and orderspecs (every #'identity orderspecs))
    `(:order-by
      (,@(nreverse
          (cl-utilities:collecting
            (let ((qualifier nil))
              (dolist (itm (nreverse orderspecs))
                (if (member itm '(:desc :asc))
                    (if qualifier
                        (error ":desc or :asc in wrong place")
                        (setf qualifier itm))
                    (if qualifier
                        (progn
                          (cl-utilities:collect (list itm qualifier))
                          (setf qualifier nil))
                        (cl-utilities:collect itm)))))))))))

;;FIXME: Postgresql specific
(defun recent-mixin (datecol agestring)
  (when (and datecol (not-empty agestring))
    `(:where ,(sql-< (sql-expression :string (format nil "age(~a)" datecol))
                     (sql-expression :string (format nil "interval '~a'" agestring))))))

(defun apply-car (data)
  (apply (symbol-function (car data)) (cdr data)))

(defun in-or-equal (col key/s)
  (if (listp key/s)
      (apply #'sql-in col (mapcar #'sql-escape key/s))
      (sql-= col (sql-escape key/s))))

(defun table-from-attribute-obj (attobj)
  (escape-error
   (case (type-of attobj)
     (clsql-sys:sql-ident-table (slot-value attobj 'clsql-sys:name))
     (clsql-sys:sql-ident-attribute (slot-value attobj 'clsql-sys::qualifier))
     (otherwise (error "Can't get table name")))))

(defun col-from-attribute-obj (attobj)
  (assert (eq (type-of attobj) 'clsql-sys:sql-ident-attribute))
  (escape-error (slot-value attobj 'clsql-sys:name)))

(def-query col-from-pkey (col key/s &key limit offset order-by)
  "Given a column with table attribute, (see colm), will extrapolate the pkey column, returning records of col where pkey is one of key/s. Key/s can be a single key or a list of keys."
  ;;FIXME: col *should* be escaped, but...
  (mapcar
   #'car
   (query-marker
    (merge-query
     (select
      col
      :from (colm (table-from-attribute-obj col))
      :where (in-or-equal
              (symbolize (get-table-pkey (table-from-attribute-obj col)))
              key/s))
     (limit-mixin limit offset)
     (order-by-mixin order-by)))))

(def-query get-pkeys-for-pkey (joinspec pkey/s &key limit offset order-by)
  "Gets foreign pkeys that are tied to a pkey across a many to many relation. Joinspec will tell which direction the relation is pointing."
  (mapcar
   #'car
   (query-marker
    (join-bind joinspec
      (merge-query
       (select
        (colm join-table fkey2)
        :from (tabl join-table)
        :where (in-or-equal (colm join-table fkey1) pkey/s)) ;:distinct t)
       (limit-mixin limit offset)
       (order-by-mixin order-by))))))

(defun update-pkeys-for-pkey (joinspec pkey pkeys)
  (with-a-database ()
    (join-bind joinspec
      (let* ((pkey (sql-escape pkey))
             (pkeys (mapcar #'string-unless-number pkeys))
             (oldpkeys (get-pkeys-for-pkey joinspec pkey))
             (newkeys (set-difference pkeys oldpkeys :test #'equal))
             (remkeys (set-difference oldpkeys pkeys :test #'equal)))
        (dolist (k newkeys)
          (insert-records
           :into (tabl join-table)
           :attributes (list (colm fkey1) (colm fkey2))
           :values (list pkey (sql-escape k))))
        (dolist (k2 remkeys)
          (delete-records
           :from (tabl join-table)
           :where (sql-and
                   (sql-= (colm fkey1) pkey)
                   (sql-= (colm fkey2) k2))))))))

(defun %%build-chain (joinspecs core)
  (labels ((make-source (comparison-column)
             (if (null (cdr joinspecs))
                 (in-or-equal comparison-column core)
                 (sql-in comparison-column
                         (apply #'sql-query
                                (cdr (%%build-chain (cdr joinspecs) core)))))))
    (let ((current (car joinspecs)))
      (cond
        ((eq (type-of current) 'clsql-sys:sql-ident-attribute)
         (unexecuted
           (select
            (colm (get-table-pkey (table-from-attribute-obj current)))
            :from (tabl (table-from-attribute-obj current))
            :where (make-source current)
            :distinct t)))
        ((= 7 (length current))
         (join-bind current
           (unexecuted
             (select
              (colm join-table fkey2)
              :from (tabl join-table)
              :where (make-source (colm join-table fkey1))
              :distinct t))))
        ((= 2 (length current))
         (if (eq (type-of (car current)) 'clsql-sys:sql-ident-table)
             (unexecuted
               (select
                (second current)
                :from (table-from-attribute-obj (second current))
                :where (make-source
                        (symbolize
                         (get-table-pkey
                          (table-from-attribute-obj (second current)))))))
             (unexecuted
               (select (sql-expression
                        :attribute
                        (get-table-pkey (table-from-attribute-obj
                                         (car current))))
                       :from (sql-expression
                              :table
                              (table-from-attribute-obj (car current)))
                       :where (make-source (car current))
                       :distinct t))))
        (t (error "Can't handle joinspec"))))))

(def-query get-pkeys-for-pkey/chain (joinspecs pkey &key limit offset order-by)
  (mapcar #'car
          (query-marker
           (merge-query
            (%%build-chain (nreverse joinspecs) (sql-escape pkey))
            (limit-mixin limit offset)
            (order-by-mixin order-by)))))

(defun get-record-by-pkey (table id)
  (with-a-database ()
    (multiple-value-bind (data labels)
        (select (sql-expression :attribute '*)
                :from (tabl table)
                :where (sql-=
                        (sql-expression :attribute
                                        (or (get-table-pkey table)
                                            (error "Table has no pkey column.")))
                        (sql-escape id)))
      (values (car data) labels))))

(defun get-assoc-by-pkey (table id)
  (apply #'pairlis
         (multiple-value-bind (a b)
             (get-record-by-pkey table id)
           (unless a
             (error "Record not found"))
           (list (mapcar (lambda (x)
                           (proto:keywordize-foreign x))
                         b) a))))

(eval-always
  (defun assocify-results (results cols)
    (let ((keys (mapcar (lambda (x) (proto:keywordize-foreign x)) cols)))
      (mapcar
       (lambda (row)
         (pairlis keys row)) results))))

(defmacro assocify (query)
  `(multiple-value-bind (results cols)
       ,query
     (assocify-results results cols)))

(defun get-assoc-by-col (colspec match/es)
  (let ((res
         (assocify
          (select '* :from (table-from-attribute-obj colspec)
                  :where (in-or-equal colspec match/es)))))
    (values (car res) res)))

(defgeneric %next-val (database sequence))
(defun next-val (sequence)
  (%next-val *default-database* sequence))

(defun update-record (table pkey values)
  "Table is a symbol, values is an alist of (column object . value) pairs"
  (with-a-database ()
    (update-records
     (tabl table)
     :av-pairs
     (mapcar (lambda (x)
               (list (sql-escape (car x))
                     (if (equal (cdr x) "") nil (cdr x))))
             values)
     :where (sql-= (tabl (get-table-pkey table))
                   (sql-escape pkey)))))


(defgeneric %insert-record (database table values))

(defun insert-record (table values)
  (%insert-record *default-database* table values))

(defun insert-plist (table values)
  (insert-record
   table
   (map-by-2 (lambda (k v) (cons (if (or (stringp k) (symbolp k)) (colm k) k) v))
             values)))

(defun insert-or-update (table key data)
  (if key
      (progn
        (update-record table key data)
        key)
      (insert-record table data)))

;;FIXME: order-by won't work with distinct this way, at least under postgres.
(def-query get-column (table col &key limit offset order-by)
  (mapcar
   #'car
   (query-marker
    (merge-query
     (select (colm col) :from (tabl table) :distinct t)
     (limit-mixin limit offset)
     (order-by-mixin order-by)))))

(def-query get-columns (table &rest cols)
  (bind-extracted-keywords (cols clean-cols :limit :offset :order-by)
    (merge-query
     `(select
       ,@(mapcar (lambda (x) (colm x)) clean-cols)
       :from ,(tabl table)
       :distinct t)
     (order-by-mixin order-by)
     (limit-mixin limit offset))))

(defun take-one (query)
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
                    (format nil "'~A'::regclass" (escape-error
                                                  (normalize-for-sql table))))))

(defun normalize-for-sql (string)
  (substitute #\_ #\- string))
;;END borrowed

;;FIXME: Security on the table symbol?
(defun get-table-pkey (table)
  (%get-table-pkey (table-symbol table) *default-database*))

(defgeneric %get-table-pkey (table database))

(defgeneric %fulltext-where (text cols database)
  (:documentation "Creates the where clause for a fulltext search of cols."))

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

(defun get-tables (&optional (database *default-database*))
  (%get-tables database))

(defgeneric %get-tables (database))
(defmethod %get-tables ((database t))
  (mapcar (lambda (x)
            (symbolize (car x) :package (find-package :keyword)))
          (select (colm 'table-name)
                  :from (colm 'information-schema 'tables)
                  :where (sql-and (sql-= (colm 'table-schema) "public")
                                  (sql-= (colm 'table-type) "BASE TABLE")))))

(defgeneric %get-table-columns (database table))
(defun get-table-columns (table)
  (%get-table-columns *default-database* table))

(defun sql-equal/null (col val)
  "Because col = null doesn't work in SQL. Use if val may be nil."
  (if val
      (sql-= col val)
      (sql-is col nil)))
