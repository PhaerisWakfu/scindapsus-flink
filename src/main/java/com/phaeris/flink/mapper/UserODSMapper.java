package com.phaeris.flink.mapper;

import com.phaeris.flink.entity.po.User;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author wyh
 * @since 2024/3/15
 */
public interface UserODSMapper {

    List<User> getUser(@Param("ids") List<Long> ids);
}
