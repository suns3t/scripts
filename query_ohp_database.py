import MySQLdb

# Remote database
RM_USER_NAME="huyhoan"
RM_USER_PWD="changethispassword"
RM_HOST_NAME="mysql.rc.pdx.edu"
rm_database_name = "ohp_commonspot"

# Local database
LC_USER_NAME = "root"
LC_USER_PWD = ""
LC_HOST_NAME = "127.0.0.1"
lc_database_name = "ohp_new"
def main():
    
    # Connect
    rm_db = MySQLdb.connect(host=RM_HOST_NAME,
            user=RM_USER_NAME,
            passwd=RM_USER_PWD,db=rm_database_name)

   

    lc_db = MySQLdb.connect(host=LC_HOST_NAME,
            user=LC_USER_NAME,
            passwd=LC_USER_PWD,
            db=lc_database_name)

    # Cursors
    lc_cursor = lc_db.cursor();
    rm_curssor = rm_db.cursor();

    # Get list of id of articles
    rm_curssor.execute("""
        SELECT 
            id, subsiteid
        FROM
            sitepages
        WHERE
            (doctype = '0') AND
            ((subsiteid <= 148 and subsiteid >= 37) OR (subsiteid = 156)) AND
            ispublic = 15
        LIMIT 50000
        """)

    # Go throught each pageid 
    for pageid in rm_curssor:
        print pageid
        # Find url, title, description
        get_page_info(pageid[0])
        #get_textblock_info(pageid[0])
        get_image_info(pageid[0],pageid[1])



def get_page_info(pageid):
    # Connect
    rm_db = MySQLdb.connect(host=RM_HOST_NAME,
            user=RM_USER_NAME,
            passwd=RM_USER_PWD,db=rm_database_name)

   

    lc_db = MySQLdb.connect(host=LC_HOST_NAME,
            user=LC_USER_NAME,
            passwd=LC_USER_PWD,
            db=lc_database_name)

    # Cursors
    lc_cursor = lc_db.cursor();
    rm_curssor = rm_db.cursor();

    is_narrative = '%/narratives/%'
    is_lesson_plan = '%/lesson-plans/%'
    is_biography = '%/biographies/%'
    is_essay = '%/essays/%'
    is_record = '%/historical-records/%'
    # Get page info
    rm_curssor.execute("""
        SELECT 
            CONCAT('www.ohs.org',subsites.subsiteurl,sitepages.filename) AS url,
            sitepages.subsiteid,
            sitepages.title,
            sitepages.description,
            IF (subsites.subsiteurl LIKE '%s',true,false) AS is_narrative,
            IF (subsites.subsiteurl LIKE '%s',true,false) AS is_lesson_plan,
            IF (subsites.subsiteurl LIKE '%s',true,false) AS is_biography,
            IF (subsites.subsiteurl LIKE '%s',true,false) AS is_essay,
            IF (subsites.subsiteurl LIKE '%s',true,false) AS is_record,
            IF (sitepages.filename = 'index.cfm',true,false) AS is_index
        FROM sitepages 
        INNER JOIN
            subsites
        ON
            subsites.id = sitepages.subsiteid
        WHERE 
            sitepages.id = %s
        """ % (is_narrative, is_lesson_plan, is_biography, is_essay, is_record, pageid))
    # Save it to local database
    # First, get url to see if it is exist or not
    result = list(rm_curssor.fetchall())
    lc_cursor.execute("""
        SELECT
            url
        FROM
            article
        WHERE
            url = '%s'
        """ % result[0][0])

    if (lc_cursor.rowcount == 0):
        # Then, if it not exist, save it to local database
        lc_cursor.execute("""
            INSERT INTO
                article (url, subsiteid, title, description, is_narrative, is_lesson_plan, is_biography, is_essay, is_record, is_index)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """ , (result[0][0], result[0][1], result[0][2], result[0][3], result[0][4], result[0][5], result[0][6], result[0][7], result[0][8], result[0][9]))

def get_image_info(pageid, subsiteid):
     # Connect
    rm_db = MySQLdb.connect(host=RM_HOST_NAME,
            user=RM_USER_NAME,
            passwd=RM_USER_PWD,db=rm_database_name)

    lc_db = MySQLdb.connect(host=LC_HOST_NAME,
            user=LC_USER_NAME,
            passwd=LC_USER_PWD,
            db=lc_database_name)

    rm_curssor.execute("""
        SELECT 
            concat('www.ohs.org',subsites.imagesurl, sitepages.filename) as src, 
            data_imagewithcaption.caption
        FROM
            data_image 
        INNER JOIN
            sitepages
        ON sitepages.id = substring(data_image.srcurl,9)
        INNER JOIN
            data_imagewithcaption
        ON data_imagewithcaption.controlid = data_image.controlid
        INNER JOIN subsites
        ON subsites.id = sitepages.subsiteid
        WHERE 
            data_image.pageid = %s
            AND data_image.datelastcurrent is null
            AND data_imagewithcaption.datelastcurrent is null
            AND sitepages.subsiteid = %s
            AND data_imagewithcaption.pageid = %s
        """ % (pageid, subsiteid, pageid))
    
    
if __name__ == "__main__":
    main()