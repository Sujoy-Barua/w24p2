package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

import javax.swing.text.html.HTML.Tag;

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
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); //   it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); //   it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
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
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            // Step 1
            
            ResultSet rst = stmt.executeQuery(
                    "SELECT First_Name " + // select first name
                            "FROM " + UsersTable + " " + // from all users
                            "ORDER BY LENGTH(FIRST_NAME) DESC");
            
            int curLength = 0;
            int largestLength = 0;
            String curString = "";
            String largestString = "";
            
            FirstNameInfo fName = new FirstNameInfo();
            
            while (rst.next()) { // step through result rows/records one by one
                curLength = rst.getString(1).length();
                curString = rst.getString(1);

                if (rst.isFirst()) { // if first record
                    largestLength = rst.getString(1).length(); //   it is the month with the most
                    largestString = rst.getString(1);

                    // add to the list
                    fName.addLongName(rst.getString(1));
                }

               
                // exit if curLength != largestLength
                if (curLength != largestLength) {
                    break;
                } else {
                    if (!curString.equals(largestString)) {
                        // add to the list
                        fName.addLongName(rst.getString(1));

                        largestString = rst.getString(1);
                    } 
                }
            }

            // Step 2: The first name(s) with the fewest letters

            rst = stmt.executeQuery(
                    "SELECT First_Name " + // select first name
                            "FROM " + UsersTable + " " + // from all users
                            "ORDER BY LENGTH(FIRST_NAME) ASC");
            
            curLength = 0;
            int shortestLength = 0;
            curString = "";
            String shortestString = "";
            
            while (rst.next()) { // step through result rows/records one by one
                curLength = rst.getString(1).length();
                curString = rst.getString(1);

                if (rst.isFirst()) { // if first record
                    shortestLength = rst.getString(1).length(); //   it is the month with the most
                    shortestString = rst.getString(1);

                    // add to the list
                    fName.addShortName(rst.getString(1));
                }

               
                // exit if curLength != 
                if (curLength != shortestLength) {
                    break;
                } else {
                    if (!curString.equals(shortestString)) {
                        // add to the list
                        fName.addShortName(rst.getString(1));

                        shortestString = rst.getString(1);
                    } 
                }
            }

            // Step 3

            
            rst = stmt.executeQuery(
                    "SELECT FIRST_NAME, COUNT(FIRST_NAME) AS fNameCount " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "GROUP BY First_Name " + // group into buckets by birth month
                            "ORDER BY fNameCount DESC"); // sort by users born in that month, descending; break ties by birth month

            
            int mostUserCount = 0;
            int curUserCount = 0;
            String mostUserName = "";
            String curUserName = "";
            
            while (rst.next()) { // step through result rows/records one by one
                curUserCount = rst.getInt(2);
                curUserName = rst.getString(1);

                if (rst.isFirst()) { // if first record
                    mostUserCount = rst.getInt(2); //   it is the month with the most
                    mostUserName = rst.getString(1);

                    // add to the list
                    fName.addCommonName(rst.getString(1));
                    fName.setCommonNameCount(rst.getInt(2));
                }

               
                // exit if curLength != largestLength
                if (curUserCount != mostUserCount) {
                    break;
                } else {
                    if (!curUserName.equals(mostUserName)) {
                        // add to the list
                        fName.addCommonName(rst.getString(1));
                        fName.setCommonNameCount(rst.getInt(2));

                        curUserName = rst.getString(1);
                    } 
                }
            }


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
            return fName; // placeholder for compilation
        } catch (SQLException e) {
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

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

        
        ResultSet rst = stmt.executeQuery(
                    "CREATE OR REPLACE VIEW People_with_Friends AS " +
                    "SELECT USER1_ID AS USER_ID " + // select first name
                            "FROM " + FriendsTable + " " +
                    "UNION " +
                    "SELECT USER2_ID AS USER_ID FROM " + FriendsTable + " ");
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */

        rst = stmt.executeQuery(
            "SELECT USER_ID, FIRST_NAME, LAST_NAME " +
            "FROM " + UsersTable + " " +
            "WHERE USER_ID NOT IN (SELECT USER_ID FROM People_with_Friends) " +
            "ORDER BY USER_ID ASC"
        );

        while (rst.next()) { // step through result rows/records one by one
            UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
            results.add(u1);
        }

        stmt.executeQuery("DROP VIEW People_with_Friends");

        } catch (SQLException e) {
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

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */

        ResultSet rst = stmt.executeQuery(
                    "SELECT U.USER_ID, U.First_Name, U.Last_Name " + // select first name
                            "FROM " + UsersTable + " U, " + CurrentCitiesTable + " C, " + HometownCitiesTable + " H " +
                    "WHERE U.USER_ID = C.USER_ID " + "AND U.USER_ID = H.USER_ID " + "AND C.CURRENT_CITY_ID != H.HOMETOWN_CITY_ID " + 
                    "ORDER BY U.USER_ID ASC"
                    );

        while (rst.next()) {
            UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
            results.add(u1);
        }

        } catch (SQLException e) {
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

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

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


            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW Popular_Photos AS " +
                "SELECT P.PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME " +
                "FROM " + PhotosTable + " P " +
                "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                "JOIN " + TagsTable + " T ON P.PHOTO_ID = T.TAG_PHOTO_ID " +
                "GROUP BY P.PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME " +
                "ORDER BY COUNT(DISTINCT T.TAG_SUBJECT_ID) DESC, P.PHOTO_ID ASC " +
                "FETCH FIRST " + num + " ROWS ONLY"
            );

            ResultSet rst = stmt.executeQuery(
                "SELECT P.PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME, " +
                "LISTAGG(T.TAG_SUBJECT_ID, ', ') WITHIN GROUP (ORDER BY T.TAG_SUBJECT_ID) AS SUBJECT_IDS, " +
                "LISTAGG(U.FIRST_NAME || ' ' || U.LAST_NAME, ', ') WITHIN GROUP (ORDER BY T.TAG_SUBJECT_ID) AS USER_NAMES " +
                "FROM " + PhotosTable + " P " +
                "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                "JOIN " + TagsTable + " T ON P.PHOTO_ID = T.TAG_PHOTO_ID " +
                "JOIN " + UsersTable + " U ON T.TAG_SUBJECT_ID = U.USER_ID " +
                "WHERE P.PHOTO_ID IN (SELECT PHOTO_ID FROM Popular_Photos) " +
                "GROUP BY P.PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME " +
                "ORDER BY P.PHOTO_ID ASC"
            );
        
            while (rst.next()) {

                PhotoInfo p = new PhotoInfo(rst.getInt(1), rst.getInt(3), rst.getString(2), rst.getString(4));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);

                String[] userIDArray = rst.getString(5).split(", ");
                String[] userNameArray = rst.getString(6).split(", ");

                for (int i = 0; i < userIDArray.length; i++) {
                    UserInfo u1 = new UserInfo(Integer.parseInt(userIDArray[i]), userNameArray[i].split(" ")[0], userNameArray[i].split(" ")[1]);
                    tp.addTaggedUser(u1);
                }


                results.add(tp);
            }

            stmt.executeQuery("DROP VIEW Popular_Photos");
        } catch (SQLException e) {
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

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
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

            // Find part A of the query
            ResultSet rst = stmt.executeQuery(
            "SELECT U1.USER_ID AS USER1_ID, U1.FIRST_NAME AS USER1_FIRST_NAME, U1.LAST_NAME AS USER1_LAST_NAME, U1.YEAR_OF_BIRTH AS USER1_YEAR_OF_BIRTH, " +
            "U2.USER_ID AS USER2_ID, U2.FIRST_NAME AS USER2_FIRST_NAME, U2.LAST_NAME AS USER2_LAST_NAME, U2.YEAR_OF_BIRTH AS USER2_YEAR_OF_BIRTH, " +
            "P.PHOTO_ID, P.PHOTO_LINK, A.ALBUM_ID, A.ALBUM_NAME " +
            "FROM " + UsersTable + " U1 " +
            "JOIN " + TagsTable + " T1 ON U1.USER_ID = T1.TAG_SUBJECT_ID " +
            "JOIN " + TagsTable + " T2 ON T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_SUBJECT_ID <> T2.TAG_SUBJECT_ID " +
            "JOIN " + UsersTable + " U2 ON T2.TAG_SUBJECT_ID = U2.USER_ID AND U1.GENDER = U2.GENDER " +
            "JOIN " + PhotosTable + " P ON T1.TAG_PHOTO_ID = P.PHOTO_ID AND T2.TAG_PHOTO_ID = P.PHOTO_ID " +
            "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
            "LEFT JOIN " + FriendsTable + " F ON U1.USER_ID = F.USER1_ID AND U2.USER_ID = F.USER2_ID OR U1.USER_ID = F.USER2_ID AND U2.USER_ID = F.USER1_ID " +
            "WHERE U1.USER_ID < U2.USER_ID AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff + " AND F.USER1_ID IS NULL " +
            "GROUP BY U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH, " +
            "P.PHOTO_ID, P.PHOTO_LINK, A.ALBUM_ID, A.ALBUM_NAME"
        );

        while (rst.next()) {
            UserInfo u1 = new UserInfo(rst.getInt("USER1_ID"), rst.getString("USER1_FIRST_NAME"), rst.getString("USER1_LAST_NAME"));
            UserInfo u2 = new UserInfo(rst.getInt("USER2_ID"), rst.getString("USER2_FIRST_NAME"), rst.getString("USER2_LAST_NAME"));
            MatchPair mp = new MatchPair(u1, rst.getInt("USER1_YEAR_OF_BIRTH"), u2, rst.getInt("USER2_YEAR_OF_BIRTH"));
            PhotoInfo p = new PhotoInfo(rst.getInt("PHOTO_ID"), rst.getInt("ALBUM_ID"), rst.getString("PHOTO_LINK"), rst.getString("ALBUM_NAME"));
            mp.addSharedPhoto(p);
            results.add(mp);
        }

        
            
        } catch (SQLException e) {
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
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
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

            
        
           stmt.executeUpdate("CREATE OR REPLACE VIEW BidirectionalFriends AS SELECT FT1.USER1_ID, FT1.USER2_ID FROM " + FriendsTable + " FT1 " +
           "UNION SELECT FT2.USER2_ID, FT2.USER1_ID FROM " + FriendsTable + " FT2");

            /*stmt.executeUpdate("CREATE VIEW commonfriendsCount AS (SELECT B1.USER1_ID, B1.USER2_ID, COUNT(*) AS num_commonfriends " +
            "FROM BidirectionalFriends B1 JOIN BidirectionalFriends B2 ON B1.USER2_ID = B2.USER2_ID AND B1.USER1_ID < B2.USER1_ID " +
            "GROUP BY B1.USER1_ID, B2.USER2_ID)");

            stmt.executeUpdate("CREATE VIEW nonfriends AS (SELECT cfc.user1_id, cfc.user2_id, cfc.num_commonfriends FROM" +
            " LEFT JOIN " + FriendsTable + " f ON cfc.user1_id = f.user1_id AND cfc.user2_id = f.user2_id WHERE f.user1_id" +
            " f.user1_id is NULL AND f.user2_id is NULL");*/


            stmt.executeUpdate("CREATE OR REPLACE VIEW TopPairs AS " +
            "WITH CommonFriendsCount AS (" +
            "    SELECT F1.USER1_ID AS User1, F2.USER1_ID AS User2, COUNT(*) AS num_commonfriends, " +
            "    LISTAGG(PU.USER_ID || ' ' || PU.FIRST_NAME || ' ' || PU.LAST_NAME, ', ') WITHIN GROUP (ORDER BY F1.USER2_ID) AS common_friend_names " +
            "    FROM BidirectionalFriends F1 " +
            "    JOIN BidirectionalFriends F2 ON F1.USER2_ID = F2.USER2_ID AND F1.USER1_ID < F2.USER1_ID " +
            "    JOIN project2.public_users PU ON F1.USER2_ID = PU.USER_ID " +
            "    GROUP BY F1.USER1_ID, F2.USER1_ID" +
            ")," +
            "NonFriends AS (" +
            "    SELECT cfc.USER1, cfc.USER2, cfc.num_commonfriends, cfc.common_friend_names " +
            "    FROM CommonFriendsCount cfc " +
            "    LEFT JOIN " + FriendsTable + " f ON cfc.USER1 = f.USER1_ID AND cfc.USER2 = f.USER2_ID " +
            "    WHERE f.USER1_ID IS NULL AND f.USER2_ID IS NULL" +
            ")" +
            "SELECT NF.USER1, U1.FIRST_NAME AS FIRST_NAME1, U1.LAST_NAME AS LAST_NAME1, " +
            "       NF.USER2, U2.FIRST_NAME AS FIRST_NAME2, U2.LAST_NAME AS LAST_NAME2, " +
            "       NF.NUM_COMMONFRIENDS, NF.COMMON_FRIEND_NAMES " +
            "FROM NonFriends NF " +
            "JOIN " + UsersTable + " U1 ON NF.USER1 = U1.USER_ID " +
            "JOIN " + UsersTable + " U2 ON NF.USER2 = U2.USER_ID "
            );

        ResultSet rst = stmt.executeQuery("SELECT * FROM TopPairs " +
                                            "ORDER BY num_commonfriends DESC, 1 ASC, 4 ASC " +
                                            "FETCH FIRST " + num +" ROWS ONLY");                                      

        
        while (rst.next()) {
            UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
            UserInfo u2 = new UserInfo(rst.getInt(4), rst.getString(5), rst.getString(6));

            UsersPair up = new UsersPair(u1, u2);
            // now get the ids of the common friends
            String[] commonFriends = rst.getString(8).split(", ");
            for (int i = 0; i < commonFriends.length; i++) {
                String[] friendInfo = commonFriends[i].split(" ");
                UserInfo u3 = new UserInfo(Integer.parseInt(friendInfo[0]), friendInfo[1], friendInfo[2]);
                up.addSharedFriend(u3);
            }
            results.add(up);
        }

        stmt.executeUpdate("DROP VIEW BidirectionalFriends");
        stmt.executeUpdate("DROP VIEW TopPairs");
        
        } catch (SQLException e) {
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
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
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
                "SELECT C.State_Name, COUNT(*) AS EventCount " + // select first name
                        "FROM " + EventsTable + " E, " + CitiesTable + " C " +
                "WHERE E.EVENT_CITY_ID = C.CITY_ID " +
                "GROUP BY C.State_Name " + 
                "ORDER BY EventCount DESC, C.State_Name ASC"
                );

        int mostEventCount = 0;

        EventStateInfo info = new EventStateInfo(50);

        while(rst.next()) {
            // get mostEventCount
            if (rst.isFirst()) {
                mostEventCount = rst.getInt(2);
                info = new EventStateInfo(mostEventCount);
            }

            // add to the list
            if (rst.getInt(2) == mostEventCount) {
                info.addState(rst.getString(1));
            } else {
                break;
            }
            
            //EventStateInfo info = new EventStateInfo(50);
            /* 
            info.addState("Kentucky");
            info.addState("Hawaii");
            info.addState("New Hampshire");
            */
        }
        
            return info; // placeholder for compilation
        } catch (SQLException e) {
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
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            ResultSet rst = stmt.executeQuery(
            "WITH UserFriends AS (" +
            "    SELECT USER1_ID AS user_id, USER2_ID AS friend_id " +
            "    FROM " + FriendsTable +
            "    WHERE USER1_ID = " + userID +
            "    UNION " +
            "    SELECT USER2_ID AS user_id, USER1_ID AS friend_id " +
            "    FROM " + FriendsTable +
            "    WHERE USER2_ID = " + userID +
            ")," +
            "FriendsAge AS (" +
            "    SELECT " +
            "        UF.friend_id, " +
            "        U.FIRST_NAME, " +
            "        U.LAST_NAME, " +
            "        U.YEAR_OF_BIRTH, " +
            "        U.MONTH_OF_BIRTH, " +
            "        U.DAY_OF_BIRTH, " +
            "        RANK() OVER (ORDER BY U.YEAR_OF_BIRTH ASC, U.MONTH_OF_BIRTH ASC, U.DAY_OF_BIRTH ASC) AS rank_oldest, " +
            "        RANK() OVER (ORDER BY U.YEAR_OF_BIRTH DESC, U.MONTH_OF_BIRTH DESC, U.DAY_OF_BIRTH DESC) AS rank_youngest " +
            "    FROM " +
            "        UserFriends UF " +
            "        JOIN " + UsersTable + " U ON UF.friend_id = U.USER_ID" +
            ")" +
            "SELECT " +
            "    FA.friend_id AS friend_id, " +
            "    FA.FIRST_NAME AS first_name, " +
            "    FA.LAST_NAME AS last_name " +
            "FROM " +
            "    FriendsAge FA " +
            "WHERE " +
            "    FA.rank_oldest = 1 OR FA.rank_youngest = 1"
            );

            while (rst.next()) {
                UserInfo young = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                rst.next();
                UserInfo old = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                return new AgeInfo(old, young);
            }

            return new AgeInfo(new UserInfo(-1, "UNWRITTEN", "UNWRITTEN"), new UserInfo(-1, "UNWRITTEN", "UNWRITTEN")); // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */

        ResultSet rst = stmt.executeQuery(
        "WITH SameLastName AS (" +
        "    SELECT U1.user_id AS user1_id, U1.last_name, U2.user_id AS user2_id" +
        "    FROM " + UsersTable + " U1" +
        "    JOIN " + UsersTable + " U2 ON U1.last_name = U2.last_name AND U1.user_id < U2.user_id" +
        ")," +
        "SameHometown AS (" +
        "    SELECT U1.user_id AS user1_id, H1.hometown_city_id, U2.user_id AS user2_id" +
        "    FROM " + HometownCitiesTable + " H1" +
        "    JOIN " + HometownCitiesTable + " H2 ON H1.hometown_city_id = H2.hometown_city_id AND H1.user_id < H2.user_id" +
        "    JOIN " + UsersTable + " U1 ON H1.user_id = U1.user_id" +
        "    JOIN " + UsersTable + " U2 ON H2.user_id = U2.user_id" +
        ")," +
        "Friends AS (" +
        "    SELECT USER1_ID AS user1_id, USER2_ID AS user2_id" +
        "    FROM " + FriendsTable + " " +
        ")," +
        "AgeDifference AS (" +
        "    SELECT U1.user_id AS user1_id, U2.user_id AS user2_id" +
        "    FROM " + UsersTable + " U1" +
        "    JOIN " + UsersTable + " U2 ON U1.user_id < U2.user_id" +
        "    WHERE ABS(U1.year_of_birth - U2.year_of_birth) < 10" +
        ")" +
        "SELECT SL.user1_id, U1.first_name, U1.last_name, SL.user2_id, U2.first_name, U2.last_name " +
        "FROM SameLastName SL " +
        "JOIN SameHometown SH ON SL.user1_id = SH.user1_id AND SL.user2_id = SH.user2_id " +
        "JOIN Friends F ON SL.user1_id = F.user1_id AND SL.user2_id = F.user2_id " +
        "JOIN AgeDifference AD ON SL.user1_id = AD.user1_id AND SL.user2_id = AD.user2_id " +
        "JOIN " + UsersTable + " U1 ON SL.user1_id = U1.user_id " +
        "JOIN " + UsersTable + " U2 ON SL.user2_id = U2.user_id " +
        "ORDER BY SL.user1_id ASC, SL.user2_id ASC"
        );

        while (rst.next()) {
            UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
            UserInfo u2 = new UserInfo(rst.getInt(4), rst.getString(5), rst.getString(6));
            SiblingInfo si = new SiblingInfo(u1, u2);
            results.add(si);
        }

        } catch (SQLException e) {
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
