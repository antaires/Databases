package uk.ac.bris.cs.databases.cwk2;

import java.sql.*;
import java.util.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import uk.ac.bris.cs.databases.api.APIProvider;
import uk.ac.bris.cs.databases.api.AdvancedForumSummaryView;
import uk.ac.bris.cs.databases.api.AdvancedForumView;
import uk.ac.bris.cs.databases.api.ForumSummaryView;
import uk.ac.bris.cs.databases.api.ForumView;
import uk.ac.bris.cs.databases.api.AdvancedPersonView;
import uk.ac.bris.cs.databases.api.PostView;
import uk.ac.bris.cs.databases.api.Result;
import uk.ac.bris.cs.databases.api.PersonView;
import uk.ac.bris.cs.databases.api.SimpleForumSummaryView;
import uk.ac.bris.cs.databases.api.SimpleTopicView;
import uk.ac.bris.cs.databases.api.SimpleTopicSummaryView;
import uk.ac.bris.cs.databases.api.TopicView;

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
        final String STMT = "SELECT username, name FROM Person";
        try (PreparedStatement p = c.prepareStatement(STMT)) {
            Map<String, String> map = new HashMap<String, String>();
            ResultSet r = p.executeQuery();
            while (r.next()){
                String username = r.getString("username");
                String name = r.getString("name");
                map.put(username, name);
            }
            p.close();
            return Result.success(map);
        } catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<PersonView> getPersonView(String username) {
        //throw new UnsupportedOperationException("Not supported yet.");
        // query to check parameters
        if (username == null || username.isEmpty()){
            return Result.failure("getPersonView: username cannot be empty");
        }

        // query to do method's job
        final String STMT = "SELECT name, id FROM Person WHERE Person.username = ?";
        try (PreparedStatement p = c.prepareStatement(STMT)) {
            //set username to ?
            p.setString(1, username);
            ResultSet r = p.executeQuery();
            PersonView pv = null;
            if (r.next()){
                String name = r.getString("name");
                String studentId = r.getString("id");
                pv = new PersonView(name, username, studentId);
            } else {
                return Result.failure("getPersonView: username not found");
            }
            p.close();
            return Result.success(pv);
        } catch (SQLException e){
            //handle exception
            return Result.fatal(e.getMessage());
        }
    }
    
    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        // query to check parameters
        if (username == null || username.isEmpty() 
             || name == null || name.isEmpty() 
             || studentId.isEmpty()){
            return Result.failure("addNewPerson: name, username, studentId cannot be empty");
        }

        //attempt to add new person, if fails, person already exists or other value error
        final String STMT = "INSERT INTO Person (name, username, studentId) VALUES (?, ?, ?)";
        try (PreparedStatement p = c.prepareStatement(STMT)) {
            p.setString(1, name);
            p.setString(2, username);
            p.setString(3, studentId);
            p.executeQuery();
            // confrim query successful
            if (p.executeUpdate() <= 0){
                return Result.failure("addNewPerson: person already exists");
            }
            p.close();
            return Result.success();
        } catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }
    
    /* A.2 */

    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {        
        // query - list of forum id, title
        List<SimpleForumSummaryView> forumList = new ArrayList<SimpleForumSummaryView>();
        final String STMT = "SELECT forumId, forumTitle FROM Forum";
        try (PreparedStatement p = c.prepareStatement(STMT)) {
            ResultSet r = p.executeQuery();
            while(r.next()){
                Integer id = r.getInt("forumId");
                String title = r.getString("forumTitle");
                SimpleForumSummaryView sfsv = new SimpleForumSummaryView(id, title);
                forumList.add(sfsv);
            }
            p.close();
            return Result.success(forumList);
        } catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result createForum(String title) {
        //throw new UnsupportedOperationException("Not supported yet.");
        // query to check parameters
        if (title == null || title.isEmpty() ){
            return Result.failure("createForum: title cannot be empty");
        }

        //attempt to add new person, if fails, person already exists or other value error
        final String STMT = "INSERT INTO Forum (title) VALUES (?)";
        try (PreparedStatement p = c.prepareStatement(STMT)) {
            p.setString(1, title);
            p.executeQuery();
            // confrim query successful
            if (p.executeUpdate() <= 0){
                return Result.failure("createForum: forum title already exists");
            }
            p.close();
            return Result.success();
        } catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
    }
 
    /* A.3 */
 
    /**
     * Get the "main page" containing a list of forums ordered alphabetically
     * by title.
     * @return the list of all forums, empty list if there are none.*/
    @Override
    public Result<List<ForumSummaryView>> getForums() {

        final String STMT = "SELECT * FROM Forum";
        try (PreparedStatement p = c.prepareStatement(STMT)){
            ResultSet r = p.executeQuery();
            List<ForumSummaryView> list = new ArrayList<ForumSummaryView>();
            ForumSummaryView fsv = null;
            while(r.next()){
                int forumId = r.getInt("forumId");
                PreparedStatement pInner = null;
                try {
                    final String STMTInner = "SELECT Topic.topicId, topicTitle, Topic.forumId from Topic " + 
                        "JOIN (SELECT Post.topicId as T, forumId as ForumId from Post " + 
                        "JOIN Topic ON Post.topicId = Topic.topicId " +
                        "WHERE ForumId = ? ORDER BY Post.timePosted DESC LIMIT 1) AS A on Topic.topicId = A.T";
                    pInner = c.prepareStatement(STMTInner);
                    pInner.setInt(1, forumId);
                    ResultSet rInner = pInner.executeQuery();
                    while(rInner.next()){
                        SimpleTopicSummaryView stvs = new SimpleTopicSummaryView(rInner.getInt("topicId"),
                                                        rInner.getInt("forumId"),
                                                        rInner.getString("topicTitle"));
                        fsv = new ForumSummaryView(forumId, r.getString("forumTitle"), stvs);
                    }
                } catch (SQLException e){
                    return Result.fatal(e.getMessage());
                }

                //if fsv is null, don't want to add it
                if (fsv != null){
                    list.add(fsv);
                }
            }
            p.close();
            return Result.success(list);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        
        /*
        List<ForumSummaryView> forumList = new ArrayList<ForumSummaryView>();
        final String STMT = "SELECT Forum.forumId AS forumId, forumTitle, topicId, topicTitle FROM Forum JOIN Topic ON Topic.forumId = Forum.forumId ORDER BY forumTitle ASC";
        try (PreparedStatement p = c.prepareStatement(STMT)) {
            ResultSet r = p.executeQuery();
            while(r.next()){
                Integer id = r.getInt("forumId");
                String title = r.getString("forumTitle");
                Integer topicId = r.getInt("topicId");
                String topicTitle = r.getString("topicTitle");
                SimpleTopicSummaryView lastTopic = new SimpleTopicSummaryView(topicId, id, topicTitle);
                ForumSummaryView fsv = new ForumSummaryView(id, title, lastTopic);
                forumList.add(fsv);
            }
            p.close();
            return Result.success(forumList);
        } catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
        */
    }   
    
    @Override
    public Result<ForumView> getForum(int id) {
        if (id <= 0){
            return Result.failure("getForum: id must be greater than zerio");
        }

        String sql = "SELECT Forum.forumTitle AS forumTitle, a.topicID AS topicID, a.topicTitle AS topicTitle, a.forumID AS forumID from Forum JOIN " +
                "(SELECT * FROM Topic WHERE forumID=? ORDER BY topicTitle) AS a " +
                "ON Forum.forumID = a.forumID";
        PreparedStatement ptmt = null;
        String forumTitle = null;
        // To test if the forum actually exists
        if (!forumExists(id)){
            return Result.failure("getForum: id does not exist");
        }
        try{
            ptmt = c.prepareStatement(sql);
            ptmt.setInt(1, id);
            ResultSet rs = ptmt.executeQuery();
            List<SimpleTopicSummaryView> topics = new ArrayList<>();
            SimpleTopicSummaryView stsv = null;
            while (rs.next()) {
                stsv = new SimpleTopicSummaryView(rs.getInt("topicID"),
                        rs.getInt("forumID"),
                        rs.getString("topicTitle"));
                topics.add(stsv);
                // If correct, the forumTitle in the result should be all the same
            }
            ForumView fv = new ForumView(id, forumTitle, topics);
            return Result.success(fv);
        }catch (SQLException e){
            return Result.fatal(e.getMessage());
        }finally {
            try{
                if (ptmt != null)
                    ptmt.close();
            }catch (SQLException e){
                return Result.fatal(e.getMessage());
            }
        }

        /*
        final String STMT = "SELECT * FROM Forum";
        try (PreparedStatement p = c.prepareStatement(STMT)){
            ResultSet r = p.executeQuery();
            List<SimpleTopicSummaryView> list = new ArrayList<SimpleTopicSummaryView>();
            ForumView fsv = null;
            int forumId; 
            while(r.next()){
                forumId = r.getInt("forumId");
                PreparedStatement pInner = null;
                // get a list of the topics in the forum, using SimpleTopicSummaryView
                try {
                    final String STMTInner = "SELECT Topic.topicId, topicTitle, Topic.forumId from Topic " + 
                        "JOIN (SELECT Post.topicId as T, forumId as ForumId from Post " + 
                        "JOIN Topic ON Post.topicId = Topic.topicId " +
                        "WHERE ForumId = ? ) AS A on Topic.topicId = A.T";
                    pInner = c.prepareStatement(STMTInner);
                    pInner.setInt(1, forumId);
                    ResultSet rInner = pInner.executeQuery();

                    // construct each topic and add to the list
                    while(rInner.next()){
                        SimpleTopicSummaryView stvs = new SimpleTopicSummaryView(rInner.getInt("topicId"),
                                                        rInner.getInt("forumId"),
                                                        rInner.getString("topicTitle"));
                        list.add(stvs);
                    }
                } catch (SQLException e){
                    return Result.fatal(e.getMessage());
                }
            }
            p.close();
            // create the Forum view with the list
            fsv = new ForumView(forumId, r.getString("forumTitle"), list);
            return Result.success(fsv);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        */

        /*
        final String STMT = "SELECT forumTitle, topicId, topicTitle FROM Forum JOIN Topic ON Topic.forumId = Forum.forumId WHERE Forum.forumId = ?";
        try (PreparedStatement p = c.prepareStatement(STMT)) {
            r.setString(1, id);
            ResultSet r = p.executeQuery();
            while(r.next()){
                String title = r.getString("forumTitle");
                // get list of topics
                SimpleTopicSummaryView = topic
                public SimpleTopicSummaryView(int topicId, int forumId, String title) 
                ForumView f = new ForumView(id, title, topics);
            }
            p.close();
            return Result.success(f);
        } catch (SQLException e){
            return Result.fatal(e.getMessage());
        }
        */
    }   

    @Override
    public Result<SimpleTopicView> getSimpleTopic(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }    
    
    @Override
    public Result<PostView> getLatestPost(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createPost(int topicId, String username, String text) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
     
    @Override
    public Result createTopic(int forumId, String username, String title, String text) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public Result<Integer> countPostsInTopic(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* B.1 */
       
    @Override
    public Result likeTopic(String username, int topicId, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public Result likePost(String username, int topicId, int post, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<List<PersonView>> getLikers(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<TopicView> getTopic(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* B.2 */

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedForumView> getAdvancedForum(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private boolean userExists(String username) {
        String sqlForUsername = "SELECT * FROM Person WHERE username=? LIMIT 1";
        PreparedStatement ptmt = null;
        try{
            boolean isEmtpy = true;
            ptmt = c.prepareStatement(sqlForUsername);
            ptmt.setString(1, username);
            ResultSet rs = ptmt.executeQuery();
            while(rs.next())
                isEmtpy = false;
            if (isEmtpy)
                return false;
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            try{
                if (ptmt != null)
                    ptmt.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        return true;
    }

    private boolean topicExists(int topicID) {
        String sqlForTopicId = "SELECT * FROM Topic WHERE topicID=? LIMIT 1";
        PreparedStatement ptmt = null;
        try{
            boolean isEmtpy = true;
            ptmt = c.prepareStatement(sqlForTopicId);
            ptmt.setInt(1, topicID);
            ResultSet rs = ptmt.executeQuery();
            while(rs.next())
                isEmtpy = false;
            if (isEmtpy)
                return false;
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            try{
                if (ptmt != null)
                    ptmt.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        return true;
    }

    private boolean forumExists(int forumID) {
        String sqlForForumID = "SELECT * FROM Forum WHERE forumID=? LIMIT 1";
        PreparedStatement ptmt = null;
        try{
            boolean isEmtpy = true;
            ptmt = c.prepareStatement(sqlForForumID);
            ptmt.setInt(1, forumID);
            ResultSet rs = ptmt.executeQuery();
            while(rs.next())
                isEmtpy = false;
            if (isEmtpy)
                return false;
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            try{
                if (ptmt != null)
                    ptmt.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        return true;
    }
}
