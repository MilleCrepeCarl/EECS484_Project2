package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT First_Name, LENGTH(First_Name) AS Length " +
                "FROM " + UsersTable + " " +
                "GROUP BY First_Name " +
                "ORDER BY LENGTH(First_Name) DESC, First_Name ASC"
            );

            //Long and Short Names
            FirstNameInfo info = new FirstNameInfo();
            long long_name_length = 0;
            long short_name_length = 0;
            if (rst.last()) {
                short_name_length = rst.getLong(2);
            }
            if (rst.first()) {
                long_name_length = rst.getLong(2);
                info.addLongName(rst.getString(1));
            }
            while (rst.next()) {
                if (rst.getLong(2) == long_name_length){
                    info.addLongName(rst.getString(1));
                }
                if (rst.getLong(2) == short_name_length){
                    info.addShortName(rst.getString(1));
                }
            }

            // Common Names
            rst = stmt.executeQuery(
                "SELECT First_Name, COUNT(*) AS Named " +
                "FROM " + UsersTable + " " +
                "GROUP BY First_Name " +
                "ORDER BY COUNT(*) DESC, First_Name ASC"
            );

            long commonCount = 0;
            if (rst.next()) {
                commonCount = rst.getLong(2);
                info.setCommonNameCount(commonCount);
                info.addCommonName(rst.getString(1));
            }
            while (rst.next()) {
                if (rst.getLong(2) == commonCount) {
                    info.addCommonName(rst.getString(1));
                }
            }

            rst.close();
            stmt.close();
            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT USER_ID, FIRST_NAME, LAST_NAME " +
                "FROM " + UsersTable + " " +
                "WHERE USER_ID NOT IN " +
                "(SELECT DISTINCT USER1_ID FROM " + FriendsTable +") " +
                "AND USER_ID NOT IN " +
                "(SELECT DISTINCT USER2_ID FROM " + FriendsTable +")"
            );

            while(rst.next()) {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                results.add(u1);
            }
            rst.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
             ResultSet rst = stmt.executeQuery(
                "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                "FROM " + UsersTable + " U, "  + CurrentCitiesTable +
                " C, " + HometownCitiesTable + " H " +
                "WHERE U.USER_ID = H.USER_ID AND H.USER_ID = C.USER_ID AND " +
                "C.CURRENT_CITY_ID != H.HOMETOWN_CITY_ID " +
                "ORDER BY U.USER_ID"
            );

            while (rst.next()) {
                UserInfo u = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                results.add(u);
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
            // Step 1
            // ------------
            // Find the  tag number of top <num> photos
            ResultSet rst = stmt.executeQuery(
                    "SELECT TagNum, pid, plink, aid, aname from ("+
                    "SELECT COUNT(t.tag_subject_id) AS TagNum, p.photo_id as pid, p.photo_link as plink, p.album_id as aid, a.album_name as aname " +
                            "FROM " + PhotosTable + " p JOIN " + TagsTable+" t " +
                            "ON p.photo_id=t.tag_photo_id "+
                            "JOIN " + AlbumsTable + " a " +
                            "ON p.album_id=a.album_id "+
                            "GROUP BY p.photo_id, p.photo_link, p.album_id, a.album_name " +
                            "ORDER BY TagNum DESC, p.photo_id ASC)"+
                    "WHERE ROWNUM<="+Integer.toString(num));

            ArrayList<TaggedPhotoInfo> tps= new ArrayList<TaggedPhotoInfo>();
            ArrayList<Long> pids= new ArrayList<Long>();
            while (rst.next()) {// step through result rows/records one by on
                long pid = rst.getLong(2);
                PhotoInfo p = new PhotoInfo(pid, rst.getLong(4), rst.getString(3), rst.getString(5));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tps.add(tp);
                pids.add(pid);

            }
            for(int i=0;i<tps.size();i++) {
                long pid=pids.get(i);
                TaggedPhotoInfo tp=tps.get(i);
                ResultSet curRst = stmt.executeQuery(
                        "SELECT u.user_id, u.first_name, u.last_name " +
                                "FROM " + PhotosTable + " p join " + TagsTable + " t " +
                                "ON p.photo_id=t.tag_photo_id " +
                                "join " + UsersTable + " u " +
                                "ON u.user_id=t.tag_subject_id " +
                                "WHERE p.photo_id=" + Long.toString(pid) + " " +
                                "ORDER by u.user_id ASC");

                while (curRst.next()) {
                    UserInfo u = new UserInfo(curRst.getLong(1), curRst.getString(2), curRst.getString(3));
                    tp.addTaggedUser(u);
                }
                // Step 4
                // ------------
                // * Close resources being used
                curRst.close();
                results.add(tp);
            }
            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }


        return results;
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
            //TODO: when no common tag, the iser pair should not be returned right?
            ResultSet rst = stmt.executeQuery(
                "SELECT UTAGS.USER1_ID, US1.FIRST_NAME, US1.LAST_NAME, US1.YEAR_OF_BIRTH, " +
                       "UTAGS.USER2_ID, US2.FIRST_NAME, US2.LAST_NAME, US2.YEAR_OF_BIRTH, " +
                       "UTAGS.TAGCOUNT, UP.PHOTO_ID, UP.PHOTO_LINK, UP.ALBUM_ID, UP.ALBUM_NAME " +
                "FROM (" +
                    "(SELECT USER1_ID, USER2_ID, TAGCOUNT " +
                    "FROM (" +
                        "SELECT X.USER1_ID AS USER1_ID, X.USER2_ID AS USER2_ID, Z.TAGCOUNT AS TAGCOUNT " +
                        "FROM (" +
                            "(SELECT USER1_ID, USER2_ID " +
                            "FROM (" +
                                "SELECT U1.USER_ID AS USER1_ID, U2.USER_ID AS USER2_ID " +
                                "FROM " + UsersTable + " U1, " + UsersTable + " U2 " +
                                "WHERE (U1.USER_ID < U2.USER_ID AND U1.GENDER = U2.GENDER AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff + ") " +
                                "MINUS " +
                                "SELECT USER1_ID, USER2_ID " +
                                "FROM " + FriendsTable +
                            ")) X " +
                            "INNER JOIN " +
                            "(SELECT USER1_ID, USER2_ID, COUNT(*) AS TAGCOUNT " +
                            "FROM ( " +
                                "SELECT T1.TAG_SUBJECT_ID AS USER1_ID, T2.TAG_SUBJECT_ID AS USER2_ID " +
                                "FROM " + TagsTable + " T1 INNER JOIN " + TagsTable + " T2 ON (T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_SUBJECT_ID < T2.TAG_SUBJECT_ID) " +
                            ") GROUP BY USER1_ID, USER2_ID) Z " +
                            "ON X.USER1_ID = Z.USER1_ID AND X.USER2_ID = Z.USER2_ID" +
                        ") " +
                        "ORDER BY Z.TAGCOUNT DESC, X.USER1_ID ASC, X.USER2_ID ASC " +
                    ") WHERE ROWNUM <= " + num +") UTAGS " +
                "LEFT JOIN " + UsersTable + " US1 ON UTAGS.USER1_ID = US1.USER_ID " +
                "LEFT JOIN project2.public_users US2 ON UTAGS.USER2_ID = US2.USER_ID) " +
                "LEFT JOIN ( " +
                    "SELECT T3.TAG_SUBJECT_ID AS USER1_ID, T4.TAG_SUBJECT_ID AS USER2_ID, P.PHOTO_ID, P.PHOTO_LINK, A.ALBUM_ID, A.ALBUM_NAME " +
                    "FROM " + TagsTable + " T3 " +
                        "INNER JOIN project2.public_tags T4 ON (T3.TAG_PHOTO_ID = T4.TAG_PHOTO_ID AND T3.TAG_SUBJECT_ID < T4.TAG_SUBJECT_ID) " +
                        "INNER JOIN project2.public_photos P ON T3.TAG_PHOTO_ID = P.PHOTO_ID " +
                        "INNER JOIN project2.public_albums A ON P.ALBUM_ID = A.ALBUM_ID " +
                    "ORDER BY P.PHOTO_ID " +
                ") UP ON UTAGS.USER1_ID = UP.USER1_ID AND UTAGS.USER2_ID = UP.USER2_ID " +
                "ORDER BY UTAGS.TAGCOUNT DESC, UTAGS.USER1_ID ASC, UTAGS.USER2_ID ASC, UP.PHOTO_ID ASC"
            );

            while (rst.next()) {
                long user_1_id = rst.getLong(1);
                long user_2_id = rst.getLong(5);
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(5), rst.getString(6), rst.getString(7));
                MatchPair mp = new MatchPair(u1, rst.getLong(4), u2, rst.getLong(8));
                PhotoInfo p = new PhotoInfo(rst.getLong(10), rst.getLong(12), rst.getString(11), rst.getString(13));
                mp.addSharedPhoto(p);
                while(rst.next()) {
                    if (user_1_id == rst.getLong(1) && user_2_id == rst.getLong(5)) {
                        PhotoInfo p1 = new PhotoInfo(rst.getLong(10), rst.getLong(12), rst.getString(11), rst.getString(13));
                        mp.addSharedPhoto(p1);
                    } else {
                        rst.previous();
                        break;
                    }
                }
                results.add(mp);
            }

            rst.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    // TODO: num is too large? Return all pairs?
    // TODO: will it be efficient to use another stmt or save result in java( which is current method)
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            ArrayList<UsersPair> ups= new ArrayList<UsersPair>();
            ArrayList<Long> i1= new ArrayList<Long>();
            ArrayList<Long> i2= new ArrayList<Long>();
            ResultSet rst = stmt.executeQuery(
                    "SELECT * from (" +
                            "SELECT mutuals.i1 as i1, u1.first_name as f1, u1.last_name as l1, mutuals.i2 as i2, u2.first_name as f2, u2.last_name as l2, count(mutuals.i3) as num "+
                            "FROM (" +
                            "SELECT f1.i1 as i1, f2.i2 as i2, f2.i1 as i3 " +
                            "FROM  ( "+
                            "(" +
                                "SELECT user1_id as i1, user2_id as i2 " +
                                "FROM " + FriendsTable +" "+
                            ")Union( "+
                                "SELECT user2_id as i1, user1_id as i2 " +
                                "FROM " + FriendsTable +" "+
                            ") "+
                            ") f1 JOIN ("+
                            "(" +
                                "SELECT user1_id as i1, user2_id as i2 " +
                                "FROM " + FriendsTable +" "+
                            ")Union( "+
                                "SELECT user2_id as i1, user1_id as i2 " +
                                "FROM " + FriendsTable +" "+
                            ") "+
                            ") f2 " +
                            "ON f1.i2=f2.i1 " +
                            "WHERE f1.i1<f2.i2 and not exists (" +
                            "SELECT * from "+FriendsTable+ " where user1_id=f1.i1 and user2_id=f2.i2 "+
                            ")"+
                            ") mutuals, "+UsersTable+" u1, "+UsersTable+" u2 "+
                            "WHERE mutuals.i1=u1.user_id and mutuals.i2=u2.user_id "+
                            "group by mutuals.i1, u1.first_name, u1.last_name, mutuals.i2, u2.first_name, u2.last_name " +
                            "ORDER by num DESC, mutuals.i1 ASC, mutuals.i2 ASC ) "+
                            "Where ROWNUM<="+Integer.toString(num)+" ");
            while(rst.next())
            {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                UsersPair up = new UsersPair(u1, u2);
                ups.add(up);
                i1.add(rst.getLong(1));
                i2.add(rst.getLong(4));
            }
            rst.close();


            for(int j=0;j<num;j++) {
                ResultSet pairRst = stmt.executeQuery(
                        "SELECT mutuals.i3, u3.first_name, u3.last_name " +
                        "FROM " +
                                "( "+
                                    "SELECT f1.i1 as i1, f2.i2 as i2, f2.i1 as i3 " +
                                    "FROM ( "+
                                            "( "+
                                                "SELECT user1_id as i1, user2_id as i2 " +
                                                "FROM " + FriendsTable +" "+
                                            ") Union" +
                                            "( "+
                                                "SELECT user2_id as i1, user1_id as i2 " +
                                                "FROM " + FriendsTable +" "+
                                            ") "+
                                    ") f1 JOIN " +
                                        "("+
                                            "(" +
                                                    "SELECT user1_id as i1, user2_id as i2 " +
                                                    "FROM " + FriendsTable +" "+
                                            ")Union" +
                                            "( "+
                                                    "SELECT user2_id as i1, user1_id as i2 " +
                                                    "FROM " + FriendsTable +" "+
                                            ") "+
                                        ") f2 " +
                                    "ON f1.i2=f2.i1 " +
                                    "WHERE f1.i1<f2.i2 and not exists (" +
                                        "SELECT * from "+FriendsTable+ " where user1_id=f1.i1 and user2_id=f2.i2 "+
                                    ")"+
                                ") "+
                        "mutuals, " + UsersTable+" u3 " +
                        "where u3.user_id=mutuals.i3 and mutuals.i1=" + Long.toString(i1.get(j))+ " and mutuals.i2="+Long.toString(i2.get(j))+" "+
                        "ORDER by mutuals.i3 ASC");

                while(pairRst.next())
                {
                    UserInfo us = new UserInfo(pairRst.getLong(1), pairRst.getString(2), pairRst.getString(3));
                    ups.get(j).addSharedFriend(us);
                }
                results.add(ups.get(j));
                pairRst.close();
            }

            stmt.close();


        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            ResultSet rst = stmt.executeQuery(
                    "SELECT MAX(COUNT(e.event_id)) " +
                            "FROM " + EventsTable + " e JOIN " + CitiesTable+" c " +
                            "ON e.event_city_id=c.city_id "+
                            "GROUP BY c.state_name");
            long maxNum=0;
            while(rst.next())
            {
                maxNum=rst.getLong(1);
            }
            ResultSet secondrst = stmt.executeQuery(
                    "SELECT c.state_name " +
                            "FROM " + EventsTable + " e JOIN " + CitiesTable+" c " +
                            "ON e.event_city_id=c.city_id "+
                            "GROUP BY c.state_name " +
                            "Having count(e.event_id)="+Long.toString(maxNum)+" "+
                            "ORDER BY c.state_name ASC");
            EventStateInfo info = new EventStateInfo(maxNum);
            while(secondrst.next())
            {
                String sName=secondrst.getString(1);
                info.addState(sName);
            }
            secondrst.close();
            rst.close();
            stmt.close();
            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }

    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    // TODO: in friends table one can not friend with self right?
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            ResultSet oldRst = stmt.executeQuery(
                    "SELECT u.user_id, u.first_name,u.last_name " +
                            "FROM "+FriendsTable+" f ,"+ UsersTable+" u " +
                            "WHERE (f.user1_id=u.user_id and f.user2_id="+ Long.toString(userID)+ " ) or ( f.user2_id=u.user_id and f.user1_id="+ Long.toString(userID)+" ) "+
                            "ORDER BY u.YEAR_OF_BIRTH ASC,u.MONTH_OF_BIRTH ASC,u.DAY_OF_BIRTH ASC, u.user_id DESC"
            );
            UserInfo old;
            UserInfo young;

            oldRst.next();
            old = new UserInfo(oldRst.getLong(1), oldRst.getString(2), oldRst.getString(3));

            ResultSet youngRst = stmt.executeQuery(
                    "SELECT u.user_id, u.first_name,u.last_name " +
                            "FROM "+FriendsTable+" f ,"+ UsersTable+" u " +
                            "WHERE (f.user1_id=u.user_id and f.user2_id="+ Long.toString(userID)+ " ) or ( f.user2_id=u.user_id and f.user1_id="+ Long.toString(userID)+" ) "+
                            "ORDER BY u.YEAR_OF_BIRTH DESC,u.MONTH_OF_BIRTH DESC,u.DAY_OF_BIRTH DESC, u.user_id DESC"
            );
            youngRst.next();
            young = new UserInfo(youngRst.getLong(1), youngRst.getString(2), youngRst.getString(3));

            oldRst.close();
            youngRst.close();
            stmt.close();
            return new AgeInfo(old, young);
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name (definitely not null)
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    // TODO: birth year be null should ignore, right?
    // TODO: if no hometown record for a user in hometown city, then it is dropped by join
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
            ResultSet rst = stmt.executeQuery(
                    "SELECT u1.user_id, u1.first_name,u1.last_name,u2.user_id, u2.first_name,u2.last_name " +
                            "FROM " + FriendsTable+" f JOIN " + UsersTable+" u1 " +
                            "ON f.user1_id=u1.user_id "+
                            "JOIN " + UsersTable+" u2 " +
                            "ON f.user2_id=u2.user_id "+
                            "JOIN " + HometownCitiesTable +" c1 "+
                            "ON u1.user_id=c1.user_id "+
                            "JOIN " + HometownCitiesTable +" c2 "+
                            "ON u2.user_id=c2.user_id "+
                            "Where u1.last_name=u2.last_name and c1.hometown_city_id=c2.hometown_city_id " +
                            "and u1.year_of_birth is not null and u2.year_of_birth is not null " +
                            "and ABS(u1.year_of_birth-u2.year_of_birth)<10 "+
                            "ORDER BY u1.user_id ASC, u2.user_id ASC"
            );
            while(rst.next())
            {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }
            rst.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
