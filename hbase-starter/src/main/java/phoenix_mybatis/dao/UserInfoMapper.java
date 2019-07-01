package phoenix_mybatis.dao;

import org.apache.ibatis.annotations.*;
import phoenix_mybatis.UserInfo;

import java.util.List;

/**
 *  on 18-3-11.
 */
@Mapper
public interface UserInfoMapper {

  @Insert("upsert into USER_INFO (ID,NAME) VALUES (#{user.id},#{user.name})")
  public void addUser(@Param("user") UserInfo userInfo);

  @Delete("delete from USER_INFO WHERE ID=#{userId}")
  public void deleteUser(@Param("userId") int userId);

  @Select("select * from USER_INFO WHERE ID=#{userId}")
  @ResultMap("userResultMap")
  public UserInfo getUserById(@Param("userId") int userId);

  @Select("select * from USER_INFO WHERE NAME=#{userName}")
  @ResultMap("userResultMap")
  public UserInfo getUserByName(@Param("userName") String userName);

  @Select("select * from USER_INFO")
  @ResultMap("userResultMap")
  public List<UserInfo> getUsers();
}
