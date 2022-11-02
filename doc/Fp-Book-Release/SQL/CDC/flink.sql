/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 80021
Source Host           : localhost:3306
Source Database       : flink

Target Server Type    : MYSQL
Target Server Version : 80021
File Encoding         : 65001

Date: 2020-10-04 18:49:34
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for `orders`
-- ----------------------------
DROP TABLE IF EXISTS `orders`;
CREATE TABLE `orders` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `total_amount` decimal(10,2) DEFAULT NULL,
  `trade_no` varchar(100) DEFAULT NULL,
  `order_status` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `user_id` bigint DEFAULT NULL,
  `payment_way` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `delivery_address` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `order_comment` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `operate_time` datetime DEFAULT NULL,
  `expire_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=481 DEFAULT CHARSET=utf8 COMMENT='订单表';

-- ----------------------------
-- Records of orders
-- ----------------------------
INSERT INTO `orders` VALUES ('1', '188.00', null, '1', '1', 'weixin', '北京王家胡同', '要顺丰', '2020-10-04 12:06:42', '2020-10-05 12:06:46', null);
INSERT INTO `orders` VALUES ('2', '2000.00', null, '1', '2', 'alipay', '上海13弄', '不限快递', '2020-10-04 12:07:49', '2020-10-04 12:07:53', null);
INSERT INTO `orders` VALUES ('3', '288.00', null, '1', '3', 'Alipay', '武汉滨江苑', '不限快递', '2020-10-04 12:58:24', '2020-10-04 12:58:27', null);
INSERT INTO `orders` VALUES ('4', '222.00', null, '1', '4', null, null, null, null, null, null);
