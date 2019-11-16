package uk.ac.bris.cs.databases.cwk2;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import uk.ac.bris.cs.databases.api.*;

/**
 *
 * @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;
    
    public API(Connection c) {
        this.c = c;
    }

    /* A.1 */
    
    @Override
    public Result<Map<String, String>> getUsers() {
        final String sql = "SELECT username, name FROM Person";
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ResultSet rs = ptmt.executeQuery();
            Map<String, String> result = new HashMap<>();
            while(rs.next()) {
                String username = rs.getString("username");
                String name = rs.getString("name");
                result.put(username, name);
            }
            return Result.success(result);
        }catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    // check every dumb thing the user can possibly do TODO
    // need close transaction if change dit in database (or rollack if error) TODO check all

    @Override
    public Result<PersonView> getPersonView(String username) {
        if (username == null || username.isEmpty()) {
            return Result.failure("getPersonView: username cannot be empty");
        }
        String sql = "SELECT username, name, stuID FROM Person " +
                "WHERE username=?";
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ptmt.setString(1, username);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()) {
                PersonView pv = new PersonView(rs.getString("name"),
                        rs.getString("username"),
                        rs.getString("stuID"));
                return Result.success(pv);
            }
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
        // Not be possible to reach this point
        return Result.fatal("Unknow error in getPersonView()");
    }
    
    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        if (name == null || name.isEmpty())
            return Result.failure("addNewPerson: Name cannot be empty");
        if (username == null || username.isEmpty())
            return Result.failure("addNewPerson: Username cannot be empty"); // TODO if studentId is null get exception this way (do want to fail if empty)
        if (studentId != null && studentId.isEmpty())
            return Result.failure("addNewPerson: Student ID cannot be empty");
        if (doesUserExist(username)) {
            return Result.failure("addNewPerson: Username already exists");
        }
        String sql = "INSERT INTO Person (name, username, stuID) VALUES (?, ?, ?)";
        // can't tell failure bc person exists vs database failure -- so check for user failure first
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ptmt.setString(1, name);
            ptmt.setString(2, username);
            if (studentId == null) {
                // check pdfs which is better TODO
                ptmt.setString(3, "");
            }else {
                ptmt.setString(3, studentId);
            }
            ptmt.executeUpdate();
            c.commit();
            return Result.success();
        }catch (SQLException e) {
            RollBack("addNewPerson");
        }
        return Result.fatal("addNewPerson: Unknown error");
    }
    
    /* A.2 */

    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {
        final String sql = "SELECT forumID, forumTitle FROM Forum";
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ResultSet rs = ptmt.executeQuery();
            List<SimpleForumSummaryView> list = new ArrayList<>();
            while(rs.next()) {
                SimpleForumSummaryView sfsv
                        = new SimpleForumSummaryView(rs.getInt("forumID"),
                        rs.getString("forumTitle"));
                list.add(sfsv);
            }
            return Result.success(list);
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result createForum(String title) {
        if (title == null || title.isEmpty())
            return Result.failure("createForum: title cannot be empty");
        if (doesForumTitleExists(title))
            return Result.failure("createForum: title already exists");
        String sql = "INSERT INTO Forum (forumTitle) VALUES (?)";
        // check that title is correct (init to null is bad pattern), put prepared statement in try, and remove savepoint
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ptmt.setString(1, title);
            ptmt.executeUpdate();
            c.commit();
            return Result.success();
        }catch (SQLException e){ // only dealing with SQL exception, means database error because already delt with user error
            RollBack("createForum");
        }
        return Result.fatal("Unknown error in createForum");
    }
 
    /* A.3 */
 
    @Override
    public Result<List<ForumSummaryView>> getForums() {
        final String sql = "SELECT * FROM Forum";
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ResultSet rs = ptmt.executeQuery();
            List<ForumSummaryView> list = new ArrayList<>();
            ForumSummaryView fsv;
            while(rs.next()) {
                int forumId = rs.getInt("forumId");
                final String sqlInner =
                        "select Topic.topicId, topicTitle, Topic.forumId from Topic " +
                                "join (select Post.topicId as T, forumID as forumID from Post join Topic on " +
                                "Post.topicID = Topic.topicID " +
                                "where forumID=? order by Post.timePosted desc limit 1) as a on Topic.topicId = a.T";
                try (PreparedStatement ptmtInner = c.prepareStatement(sqlInner)){
                    ptmtInner.setInt(1, forumId);
                    ResultSet rsInner = ptmtInner.executeQuery();
                    SimpleTopicSummaryView stsv = null;
                    while (rsInner.next()) {
                        stsv =
                                new SimpleTopicSummaryView(rsInner.getInt("topicId"),
                                        rsInner.getInt("forumId"),
                                        rsInner.getString("topicTitle"));
                    }
                    fsv = new ForumSummaryView(forumId, rs.getString("forumTitle"), stsv);
                }catch (SQLException e){
                    return Result.fatal(e.getMessage());
                }
                // If a forum actually has a topic that has post, we then add to the list,
                // Otherwise the list will be empty, rather a list of null
                if (fsv != null){
                    list.add(fsv);
                }
            }
            return Result.success(list);
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }   
    
    @Override
    public Result<ForumView> getForum(int id) {
        if (id <= 0) {
            return Result.failure("getForum: id cannot be less or equal to zero");
        }
        String sql = "SELECT Forum.forumTitle AS forumTitle, a.topicID AS topicID, a.topicTitle AS topicTitle, a.forumID AS forumID from Forum JOIN " +
                "(SELECT * FROM Topic WHERE forumID=? ORDER BY topicTitle) AS a " +
                "ON Forum.forumID = a.forumID";
        String titleSql = "SELECT * FROM Forum WHERE forumID=?";
        String forumTitle = "";
        // To test if the forum actually exists
        if (!doesForumExist(id)){ // add if does exist, get back id TODO - not here but in other cases
            return Result.failure("getForum: id does not exist");
        }
        try(PreparedStatement ptmt1 = c.prepareStatement(titleSql)){
            ptmt1.setInt(1, id);
            ResultSet rs1 = ptmt1.executeQuery();
            if (rs1.next()){ // if else here instead TODO - check everywhere fo rif/else
                forumTitle = rs1.getString("forumTitle");
            }
        }catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ptmt.setInt(1, id);
            ResultSet rs = ptmt.executeQuery();
            List<SimpleTopicSummaryView> topics = new ArrayList<>();
            //SimpleTopicSummaryView stsv = null;
            if (rs.next()) {
                SimpleTopicSummaryView stsv = new SimpleTopicSummaryView(rs.getInt("topicID"),
                        rs.getInt("forumID"),
                        rs.getString("topicTitle"));
                topics.add(stsv);
            }
            ForumView fv = new ForumView(id, forumTitle, topics);
            return Result.success(fv);
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }   

    @Override
    public Result<SimpleTopicView> getSimpleTopic(int topicId) {
        if (topicId <= 0)
            return Result.failure("getSimpleTopic: topic Id cannot be less or equal to 0");
        String sql = "SELECT Topic.topicID AS topicId, Topic.topicTitle AS topicTitle, " +
                "Post.authorUsername AS authorUsername, Post.postText AS postText, Post.timePosted " +
                "AS postedAt, Post.postNumberinTopic AS n FROM Post JOIN Topic ON Post.topicID = Topic.topicID WHERE Topic.topicID = ?";
        String topicTitle;
        // Do verification
        if (!doesTopicExist(topicId)){
            return Result.failure("getSimpleTopic: id does not exist");
        }
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ptmt.setInt(1, topicId);
            ResultSet rs = ptmt.executeQuery();
            List<SimplePostView> list = new ArrayList<>();
            if (rs.next()) {
                SimplePostView spv = new SimplePostView(rs.getInt("n"), rs.getString("authorUsername"),
                        rs.getString("postText"), rs.getString("postedAt"));
                list.add(spv);
                topicTitle = rs.getString("topicTitle");
                SimpleTopicView stv = new SimpleTopicView(topicId, topicTitle, list);
                return Result.success(stv);
            }
            return Result.failure("Unknown error in getSimpleTopic");
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }    
    
    @Override
    public Result<PostView> getLatestPost(int topicId) {
        if (topicId <= 0)
            return Result.failure("getLatestPost: topic ID cannot be less or equal to 0");
        if (!doesTopicExist(topicId)) {
            return Result.failure("getLatestPost: topicId does not exist");
        }
        String sql = "SELECT Person.name, Person.username, b.* FROM Person JOIN " +
                "(SELECT a.numOfLikes, a.postID, a.topicID, a.topicTitle, " +
                "a.authorUsername, a.postText, a.postedAt, Forum.forumID, Forum.forumTitle FROM Forum " +
                "JOIN " +
                "(SELECT Topic.forumID AS forumID, Topic.topicID AS topicId, Topic.topicTitle AS topicTitle, " +
                "Post.authorUsername AS authorUsername, Post.postText AS postText, Post.timePosted " +
                "AS postedAt, Post.postID AS postID, Post.numOfLikes AS numOfLikes FROM Post " +
                "JOIN " +
                "Topic ON Post.topicID = Topic.topicID WHERE Topic.topicID = ? ORDER BY Post.timePosted) AS a " +
                "ON a.forumID = Forum.forumID) AS b " +
                "ON b.authorUsername = Person.username ";
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ptmt.setInt(1, topicId);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()) {
                PostView pv = new PostView(rs.getInt("forumID"), rs.getInt("topicID"),
                        rs.getInt("postID"), rs.getString("name"),
                        rs.getString("authorUsername"), rs.getString("postText"),
                        rs.getString("postedAt"), rs.getInt("numOfLikes"));
                return Result.success(pv);
            }
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
        return Result.fatal("getLatestPost: Uknown Error");
    }

    @Override
    public Result createPost(int topicId, String username, String text) {
        if (topicId <= 0)
            return Result.failure("createPost: topic Id cannot be less than or equal to 0");
        if (username == null || username.isEmpty())
            return Result.failure("createPost: username cannot be empty");
        if (text == null || text.isEmpty())
            return Result.failure("createPost: text cannot be empty");
        if (!doesTopicExist(topicId)) {
            return Result.failure("createPost: topicId does not exist");
        }
        if (!doesUserExist(username)) {
            return Result.failure("createPost: username does not exist");
        }
        String checkNumber = "SELECT * FROM Post WHERE topicID=? ORDER BY postNumberinTopic DESC LIMIT 1";
        String sql = "INSERT INTO Post (authorUsername, postText, timePosted, topicID, postNumberinTopic) " +
                "VALUES(?,?,?,?,?)";
        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String now = sdf.format(d.getTime());
        int number = 1;
        try(PreparedStatement ptmt = c.prepareStatement(checkNumber)){
            ptmt.setInt(1, topicId);
            ResultSet rs = ptmt.executeQuery();
            while (rs.next()) {
                number = rs.getInt("postNumberinTopic");
            }
            try (PreparedStatement ptmt1 = c.prepareStatement(sql)) {
                ptmt1.setString(1, username);
                ptmt1.setString(2, text);
                ptmt1.setString(3, now);
                ptmt1.setInt(4, topicId);
                ptmt1.setInt(5, number + 1);
                ptmt1.executeUpdate();
                c.commit();
            } catch (SQLException e) {
                return Result.fatal("createPost:" + e.getMessage());
            }
            String sql1 = "UPDATE Topic SET lastPostTime=? WHERE topicID=?";
            try (PreparedStatement ptmt2 = c.prepareStatement(sql1)) {
                ptmt2.setString(1, now);
                ptmt2.setInt(2, topicId);
                ptmt2.executeUpdate();
                c.commit();
            }
            return Result.success();
        }catch (SQLException e){
            RollBack("createPost");
        }
        return Result.fatal("create Post: Unknown Error");
    }
     
    @Override
    public Result createTopic(int forumId, String username, String title, String text) {
        if (forumId <= 0) //TODO : sonam method object....
            return Result.failure("createTopic: forum Id cannot be less than or equal to 0");
        if (username == null || username.isEmpty())
            return Result.failure("createTopic: username cannot be empty");
        if (title == null || title.isEmpty())
            return Result.failure("createTopic: title cannot be empty");
        if (text == null || text.isEmpty())
            return Result.failure("createTopic: text cannot be empty");
        if (!doesForumExist(forumId)) {
            return Result.failure("createTopic: forum does not exist");
        }
        if (!doesUserExist(username)) {
            return Result.failure("createPost: username does not exist");
        }
        if (doesTopicTitleExist(forumId, title)){
            return Result.failure("createPost: topic title already exists");
        }
        String sql = "INSERT INTO Topic (topicTitle, forumID, timeCreated, lastPostTime, createdBy) VALUES " +
                "(?,?,?,?,?)";
        try(PreparedStatement ptmt = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            // Create a new topic
            ptmt.setString(1, title);
            ptmt.setInt(2, forumId);
            Date d = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now = sdf.format(d.getTime());
            ptmt.setString(3, now);
            ptmt.setString(4, now);
            ptmt.setString(5, username);
            ptmt.executeUpdate();
            ResultSet rs = ptmt.getGeneratedKeys();
            int generatedTopicID = -1; // A symbol of error
            while (rs.next())
                generatedTopicID = rs.getInt(1);
            createPost(generatedTopicID, username, text);
            c.commit();
            return Result.success();
        }catch (SQLException e){
            RollBack("createTopic");
        }
        return Result.fatal("createTopic: Unknown Error");
    }
    
    @Override
    public Result<Integer> countPostsInTopic(int topicId) {
        if (topicId <= 0)
            return Result.failure("countPostsInTopic: topic Id cannot be less than or equal to 0");
        if (!doesTopicExist(topicId)) {
            return Result.failure("countPostsInTopic: username does not exist");
        }
        String sql = "SELECT COUNT(*) FROM (SELECT Post.postID " +
                "FROM Post JOIN Topic ON Post.topicID = Topic.topicID " +
                "where Topic.topicID = ?) AS a";
        try(PreparedStatement ptmt = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            ptmt.setInt(1, topicId);
            ResultSet rs = ptmt.executeQuery();
            int count = 0; // A count of 0 symbolizes no posts
            while(rs.next())
                count = rs.getInt(1);
            return Result.success(count);
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }

    /* B.1 */
       
    @Override
    public Result likeTopic(String username, int topicId, boolean like) {
        if (username == null || username.isEmpty())
            return Result.failure("likeTopic: username cannot be empty");
        if (topicId <= 0)
            return Result.failure("likeTopic: topic ID cannot be less than or equal to 0");
        if (!doesTopicExist(topicId))
            return Result.failure("likeTopic: topic does not exist");
        if (!doesUserExist(username))
            return Result.failure("likeTopic: username does not exist");
        String sql = "SELECT * FROM TopicLike WHERE topicID=? AND username=? LIMIT 1";
        try (PreparedStatement ptmt = c.prepareStatement(sql)){
            if (like) {
                ptmt.setInt(1, topicId);
                ptmt.setString(2, username);
                ResultSet rs = ptmt.executeQuery();
                if (rs.next()){
                    return Result.success();
                }
                String sqlInsert = "INSERT INTO TopicLike (username, topicID) VALUES(?, ?)";
                try(PreparedStatement ptmt1 = c.prepareStatement(sqlInsert)) {
                    ptmt1.setString(1, username);
                    ptmt1.setInt(2, topicId);
                    ptmt1.executeUpdate();
                    c.commit();
                    return Result.success();
                } catch (SQLException e){
                    return Result.fatal("likeTopic: " + e.getMessage());
                }
            }else{
                String sql2 = "SELECT * FROM TopicLike WHERE topicID=? AND username=? LIMIT 1";
                try(PreparedStatement ptmt2 = c.prepareStatement(sql2)) {
                    ptmt2.setInt(1, topicId);
                    ptmt2.setString(2, username);
                    ResultSet rs = ptmt.executeQuery();
                    boolean isEmpty = true;
                    while (rs.next())
                        isEmpty = false;
                    if (isEmpty)
                        return Result.success();
                } catch (SQLException e){
                    return Result.fatal("likeTopic: " + e.getMessage());
                }
                String sqlDelete = "DELETE FROM TopicLike WHERE username=? AND topicID=?";
                try (PreparedStatement ptmt3 = c.prepareStatement(sqlDelete)){
                    ptmt.setString(1, username);
                    ptmt.setInt(2, topicId);
                    ptmt.executeUpdate();
                    c.commit();
                    return Result.success();
                } catch (SQLException e){
                    return Result.fatal("likeTopic: " + e.getMessage());
                }
            }
        }catch (SQLException e){
            RollBack("likeTopic");
            return Result.fatal(e.getMessage());
        }
    }
    
    @Override
    public Result likePost(String username, int topicId, int post, boolean like) {
        if (username == null || username.isEmpty())
            return Result.failure("likePost: username cannot be empty");
        if (topicId <= 0)
            return Result.failure("likePost: topic ID cannot be less than or equal to 0");
        if (post <= 0)
            return Result.failure("likePost: post ID cannot be less than or equal to 0");
        if (!doesTopicExist(topicId))
            return Result.failure("likePost: topic does not exist");
        if (!doesUserExist(username))
            return Result.failure("likePost: username does not exist");
        int postID;
        if ((postID = doesPostExist(topicId, post)) == -1){
            return Result.failure("likePost: post ID does not exist");
        }
        // Test if post exists in the topic with topicId and search for postID
        // Then do the operation
        String sql = "SELECT * FROM PostLike WHERE postID=? AND username=? LIMIT 1";
        try (PreparedStatement ptmt =  c.prepareStatement(sql)){
            ptmt.setInt(1, postID);
            ptmt.setString(2, username);
            if (like) {
                ptmt.setInt(1, postID);
                ptmt.setString(2, username);
                ResultSet rs = ptmt.executeQuery();
                if(rs.next())
                    return Result.success();
                String sqlInsert = "INSERT INTO PostLike (username, postID) VALUES(?, ?)";
                try(PreparedStatement ptmt1 = c.prepareStatement(sqlInsert)) {
                    ptmt1.setString(1, username);
                    ptmt1.setInt(2, postID);
                    ptmt1.executeUpdate();
                    c.commit();
                    return Result.success();
                }catch (SQLException e) {
                    return Result.fatal(e.getMessage());
                }
            }else{
                ptmt.setInt(1, postID);
                ptmt.setString(2, username);
                ResultSet rs = ptmt.executeQuery();
                boolean isEmpty1 = true;
                while(rs.next())
                    isEmpty1 = false;
                if (isEmpty1)
                    return Result.success();
                String sqlDelete = "DELETE FROM PostLike WHERE username=? AND postID=?";
                try(PreparedStatement ptmt2 = c.prepareStatement(sqlDelete)){
                    ptmt2.setString(1, username);
                    ptmt2.setInt(2, postID);
                    ptmt2.executeUpdate();
                    c.commit();
                    return Result.success();
                }catch (SQLException e) {
                    return Result.fatal(e.getMessage());
                }
            }
        }catch (SQLException e){
            RollBack("likePost");
        }
        return Result.fatal("likePost: Unknown Error");
    }

    @Override
    public Result<List<PersonView>> getLikers(int topicId) {
        if (topicId <= 0)
            return Result.failure("getLikers: topic ID cannot be less than or equal to 0");
        if (!doesTopicExist(topicId))
            return Result.failure("getLikers: topic does not exist");
        List<PersonView> list = new ArrayList<>();
        String sql = "SELECT Person.username AS username, Person.name AS name, Person.stuID AS stuID FROM Person " +
                "JOIN TopicLike ON Person.username = TopicLike.username " +
                "WHERE TopicLike.topicID = ? ORDER BY username";
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ptmt.setInt(1, topicId);
            ResultSet rs = ptmt.executeQuery();
            while (rs.next()){
                PersonView p = new PersonView(rs.getString("name"),
                        rs.getString("username"), rs.getString("stuID"));
                list.add(p);
            }
            return Result.success(list);
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<TopicView> getTopic(int topicId) {
        if (topicId <= 0)
            return Result.failure("getTopic: topic ID cannot be less than or equal to 0");
        if (!doesTopicExist(topicId))
            return Result.failure("getTopic: topic does not exist");
        List<PostView> list = new ArrayList<>();
        String sql1 = "SELECT b.*, Person.name AS name FROM Person JOIN  " +
                "(SELECT a.postID, a.authorUsername AS username, a.postText AS postText, a.timePosted AS postedAt, " +
                "a.postNumberinTopic as n, Topic.forumID FROM Topic JOIN  " +
                "(SELECT * FROM Post WHERE topicID = ? ORDER BY timePosted ASC) AS a " +
                "ON Topic.topicID = a.topicID) AS b " +
                "ON Person.username = b.username";
        try(PreparedStatement ptmt = c.prepareStatement(sql1)){
            ptmt.setInt(1, topicId);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()){
                int postId = rs.getInt("postID");
                String sql2 = "SELECT COUNT(*) AS c FROM (SELECT * FROM PostLike WHERE postID=?) AS A";
                try(PreparedStatement ptmt2 = c.prepareStatement(sql2)) {
                    ptmt2.setInt(1, postId);
                    ResultSet rsTemp = ptmt2.executeQuery();
                    int likes = 0;
                    while (rsTemp.next())
                        likes = rsTemp.getInt("c");
                    PostView pv = new PostView(rs.getInt("forumID"), topicId,
                            rs.getInt("n"),
                            rs.getString("name"), rs.getString("username"),
                            rs.getString("postText"), rs.getString("postedAt"),
                            likes);
                    list.add(pv);
                }
            }
            String sql3 = "SELECT A.*, Forum.forumTitle FROM Forum JOIN  " +
                    "(SELECT * FROM Topic WHERE topicID = ?) AS A " +
                    "ON Forum.forumID = A.forumID";
            try(PreparedStatement ptmt3 = c.prepareStatement(sql3)) {
                ptmt3.setInt(1, topicId);
                ResultSet rs1 = ptmt3.executeQuery();
                if (rs1.next()) {
                    TopicView tv = new TopicView(rs1.getInt("forumID"), rs1.getInt("topicID"),
                            rs1.getString("forumTitle"), rs1.getString("topicTitle"),
                            list);
                    return Result.success(tv);
                }
            } catch (SQLException e){
                return Result.fatal("getTopic: " + e.getMessage());
            }
        }catch (SQLException e){
            return Result.fatal("getTopic: " + e.getMessage());
        }
        return Result.fatal("getTopic: Unknown Error");
    }

    /* B.2 */

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
        if (username == null || username.isEmpty())
            return Result.failure("getAdvancedPersonView: username cannot be empty");
        if (!doesUserExist(username))
            return Result.failure("getAdvancedPersonView: username does not exist");
        String sql1 = "SELECT * FROM Person WHERE username=?";
        try(PreparedStatement ptmt = c.prepareStatement(sql1)) {
            String name = null, studentId = null;
            ptmt.setString(1, username);
            ResultSet rs1 = ptmt.executeQuery();
            if (rs1.next()){
                name = rs1.getString("name");
                studentId = rs1.getString("stuID");
            }
            String sql2 = "SELECT COUNT(*) AS n FROM " +
                    "(SELECT PostLike.username FROM PostLike JOIN " +
                    "(SELECT postID FROM Post WHERE authorUsername=?) AS a " +
                    "on a.postID=PostLike.postID) AS b";
            int postLikeCount = 0;
            try (PreparedStatement ptmt2 = c.prepareStatement(sql2)) {
                ptmt2.setString(1, username);
                ResultSet rs2 = ptmt2.executeQuery();
                if (rs2.next())
                    postLikeCount = rs2.getInt("n");
            }catch (SQLException e) {
                return Result.fatal("getAdvancedPersonView: " + e.getMessage());
            }
            // If a topic is started by him/her, the first post is his/hers
            String sql3 = "select COUNT(*) as n from " +
                    "(select TopicLike.username from TopicLike join " +
                    "(select topicID from Topic where createdBy=?) as a " +
                    "on a.topicID=TopicLike.topicID) as b";
            int topicLikeCount = 0;
            try(PreparedStatement ptmt3 = c.prepareStatement(sql3)) {
                ptmt3.setString(1, username);
                ResultSet rs3 = ptmt3.executeQuery();
                if (rs3.next())
                    topicLikeCount = rs3.getInt("n");
            }catch (SQLException e) {
                return Result.fatal("getAdvancedPersonView: " + e.getMessage());
            }
            // Except likes
            String sql4 = "select i.*, o.postCount from " +
                    "((select count(*) as postCount, Post.topicID from Post group by topicID) as o " +
                    "join " +
                    "(select q.*, p.likes from " +
                    "(select c.*, Person.name from Person join " +
                    "(select b.*, Post.authorUsername as lastPostName from Post join " +
                    "(select Topic.* from Topic join " +
                    "(select topicID from TopicLike where username=?) as a " +
                    "where Topic.topicID=a.topicID) as b " +
                    "on Post.topicID=b.topicID and timePosted=b.lastPostTime) as c " +
                    "on Person.username=c.createdBy order by c.topicTitle) as q " +
                    "JOIN " +
                    "(select d.topicID, count(*) as likes from TopicLike join " +
                    "(select c.*, Person.name from Person join " +
                    "(select b.*, Post.authorUsername as lastPostName from Post join " +
                    "(select Topic.* from Topic join " +
                    "(select topicID from TopicLike where username=?) as a " +
                    "where Topic.topicID=a.topicID) as b " +
                    "on Post.topicID=b.topicID and timePosted=b.lastPostTime) as c " +
                    "on Person.username=c.createdBy order by c.topicTitle) as d " +
                    "on d.topicID=TopicLike.topicID group by d.topicID) as p " +
                    "on q.topicID=p.topicID) as i " +
                    "on o.topicID=i.topicID)";
            try(PreparedStatement ptmt4 = c.prepareStatement(sql4)) {
                ptmt4.setString(1, username);
                ptmt4.setString(2, username);
                List<TopicSummaryView> list = new ArrayList<>();
                ResultSet rs4 = ptmt4.executeQuery();
                while (rs4.next()) {
                    TopicSummaryView t = new TopicSummaryView(rs4.getInt("topicID"),
                            rs4.getInt("forumID"), rs4.getString("topicTitle"),
                            rs4.getInt("postCount"), rs4.getString("timeCreated"), rs4.getString("lastPostTime"),
                            rs4.getString("lastPostName"), rs4.getInt("likes"),
                            rs4.getString("name"), rs4.getString("createdBy"));
                    list.add(t);
                }
                AdvancedPersonView ap = new AdvancedPersonView(name, username, studentId, topicLikeCount,
                        postLikeCount, list);
                return Result.success(ap);
            }
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<AdvancedForumView> getAdvancedForum(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private boolean doesUserExist(String username) {
        String sqlForUsername = "SELECT * FROM Person WHERE username=? LIMIT 1";
        try(PreparedStatement ptmt = c.prepareStatement(sqlForUsername)){
            ptmt.setString(1, username);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()) {
                return true;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    private boolean doesTopicExist(int topicID) {
        String sqlForTopicId = "SELECT * FROM Topic WHERE topicID=? LIMIT 1";
        try(PreparedStatement ptmt = c.prepareStatement(sqlForTopicId)){
            ptmt.setInt(1, topicID);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()) {
                return true;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    private boolean doesForumExist(int forumID) {
        String sqlForForumID = "SELECT * FROM Forum WHERE forumID=? LIMIT 1";
        try(PreparedStatement ptmt = c.prepareStatement(sqlForForumID)){
            ptmt.setInt(1, forumID);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()) {
                return true;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    private boolean doesForumTitleExists(String forumTitle) {
        String sqlForForumTitle = "SELECT * FROM Forum WHERE forumTitle=? LIMIT 1";
        try(PreparedStatement ptmt = c.prepareStatement(sqlForForumTitle)){
            ptmt.setString(1, forumTitle);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()) {
                return true;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    private Result RollBack(String methodName) {
        try {
            c.rollback();
        }catch (SQLException e) {
            return Result.fatal(methodName + e.getMessage());
        }
        return Result.fatal(methodName + ": Unknown error on rollback");
    }

    private boolean doesTopicTitleExist(int forumId, String title){
        String sqlForTopicTitle = "SELECT * FROM Topic WHERE forumID=? LIMIT 1";
        try(PreparedStatement ptmt = c.prepareStatement(sqlForTopicTitle)){
            ptmt.setInt(1, forumId);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()) {
                return true;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    private int doesPostExist(int topicId, int post){
        String sql = "SELECT * FROM Post WHERE topicID=? AND postNumberinTopic=? LIMIT 1";
        try(PreparedStatement ptmt = c.prepareStatement(sql)){
            ptmt.setInt(1, topicId);
            ptmt.setInt(2, post);
            ResultSet rs = ptmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("postID");
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return -1;
    }

}
