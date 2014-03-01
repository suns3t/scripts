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
        # Find url, title, description
        inserted_page_id = get_page_info(pageid[0])
        print '%s - %s' % (pageid[0],inserted_page_id)
        get_image_info(pageid[0], inserted_page_id)
        get_textblock_info(pageid[0], inserted_page_id)



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
    
    # Select to see if the url existed or not
    lc_cursor.execute("""
        SELECT
            id
        FROM
            article
        WHERE
            url = '%s'
        """ % result[0][0])
    
    #import pdb; pdb.set_trace();
    
    if (lc_cursor.rowcount == 0):
        # Then, if it not exist, save it to local database and return the inserted row id
        lc_cursor.execute("""
            INSERT INTO
                article (url, subsiteid, title, description, is_narrative, is_lesson_plan, is_biography, is_essay, is_record, is_index)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """ , (result[0][0], result[0][1], result[0][2], result[0][3], result[0][4], result[0][5], result[0][6], result[0][7], result[0][8], result[0][9]))
        return lc_cursor.lastrowid
    else:
        # Else, return the id of that row
        return list(lc_cursor.fetchone())[0]

def get_image_info(pageid, inserted_page_id):
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
    
    # Step 1 - Get image data 
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
            data_image.pageid = data_imagewithcaption.pageid 
            AND data_image.datelastcurrent is null
            AND data_imagewithcaption.datelastcurrent is null
            AND data_imagewithcaption.pageid = %s
        LIMIT 50000
        """ % (pageid))

    # Step 2 - Save data into local database
    for image_data in rm_curssor:
        # Step 2a - Check if the data exists or not
        lc_cursor.execute("""
            SELECT
                id
            FROM 
                image
            WHERE
                src = '%s'
            """ % image_data[0])

        if (lc_cursor.rowcount == 0):
            # Step 2b - Insert new data since it not exist.
            lc_cursor.execute("""
                    INSERT INTO
                        image (src, caption)
                    VALUES 
                        (%s, %s)
                """ , (image_data[0], image_data[1]))

            # Step 2c - Insert new data into article_image table
            image_id = lc_cursor.lastrowid

            lc_cursor.execute("""
                    INSERT INTO
                        article_image (article_id, image_id)
                    VALUES
                        (%s, %s)
                """ , (inserted_page_id, image_id))
        else:
            # If the data of the image exist, maybe it is also used in other article
            # Let check article_image table first
            for image_id in lc_cursor:
                lc_cursor.execute("""
                        SELECT
                            id
                        FROM
                            article_image
                        WHERE
                            article_id = '%s' AND
                            image_id = '%s'
                    """ % (inserted_page_id, image_id[0]))

                if (lc_cursor.rowcount == 0):
                    lc_cursor.execute("""
                        INSERT INTO
                            article_image (article_id, image_id)
                        VALUES
                            (%s, %s)
                    """ , (inserted_page_id, image_id[0]))


def get_textblock_info(pageid, inserted_page_id):
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
    rm_cursor = rm_db.cursor();
    
    # Step 1 - Get textblock data
    rm_cursor.execute("""
        SELECT 
            caption, textblock
        FROM 
            data_textblock
        WHERE 
            pageid = %s
            AND datelastcurrent is null
        ;
        """ % pageid)

    for textblock_data in rm_cursor:
        lc_cursor.execute("""
                SELECT
                    id
                FROM
                    textblock
                WHERE
                    caption = '%s'
                    AND textblock = '%s'
            """ % (textblock_data[0], textblock_data[1]))

        # If the textblock does not exist in textblock table, insert data into textblock and 
        # article_textblock tables
        if (lc_cursor.rowcount == 0):
            lc_cursor.execute("""
                    INSERT INTO
                        textblock (caption, textblock)
                    VALUES
                        (%s, %s)
                """, (textblock_data[0], textblock_data[1]))

            textblock_id = lc_cursor.lastrowid

            lc_cursor.execute("""
                    INSERT INTO
                        article_textblock (article_id, textblock_id)
                    VALUES
                        (%s, %s)
                """, (inserted_page_id, textblock_id))
        else:
            # If the textblock is exactly similar to an exist textblock, insert into 
            # article_textblock table only.
            for textblock_id in lc_cursor:
                lc_cursor.execute("""
                        SELECT
                            id
                        FROM
                            article_textblock
                        WHERE
                            article_id = '%s' AND
                            textblock_id = '%s'
                    """ % (inserted_page_id, textblock_id[0]))

                if (lc_cursor.rowcount == 0):
                    lc_cursor.execute("""
                            INSERT INTO
                                article_textblock (article_id, textblock_id)
                            VALUES
                                (%s, %s)
                        """, (inserted_page_id, textblock_id[0]))

if __name__ == "__main__":
    main()