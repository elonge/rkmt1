import { prop } from "@typegoose/typegoose/lib/prop";
import { Field, ObjectType, registerEnumType } from "type-graphql";
import {
  DocumentType,
  getModelForClass,
  Ref,
  index,
} from "@typegoose/typegoose";
import { Group } from "./group.model";
// import { GroupParticipant } from "whatsapp-web.js";
// import { PastGroupParticipant } from "./data/group-past-paricipant";

@index({ "groups.group": 1 })
@ObjectType()
export class UserProfile {
  @Field(() => String, { nullable: false })
  @prop({ type: () => String, index: true, unique: true })
  userId!: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  name?: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  status?: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  profilePicUrl: string;

  @Field(() => [GroupMembership], { nullable: true })
  @prop({ type: () => [GroupMembership], required: false })
  groups: GroupMembership[];
}
enum GroupMembershipRole {
  ADMIN = "ADMIN",
  MEMBER = "MEMBER",
  SUPER_ADMIN = "SUPER_ADMIN",
}

enum GroupMembershipStatus {
  JOINED = "JOINED",
  LEFT = "LEFT",
}
registerEnumType(GroupMembershipRole, {
  name: "GroupMembershipRole",
  description: "Roles of a user in a group",
});
registerEnumType(GroupMembershipStatus, {
  name: "GroupMembershipStatus",
  description: "Status of a user in a group",
});

@ObjectType()
export class GroupMembership {
  @Field(() => Group, { nullable: false })
  @prop({ ref: () => Group, required: true })
  group: Ref<Group>;

  @Field(() => Date, { nullable: true })
  @prop({ type: () => Date, required: false })
  joinedAt?: Date;

  @Field(() => Date, { nullable: true })
  @prop({ type: () => Date, required: false })
  leftAt?: Date;

  @Field(() => GroupMembershipRole, { nullable: false })
  @prop({ enum: GroupMembershipRole, required: true })
  role: GroupMembershipRole;

  @Field(() => GroupMembershipStatus, { nullable: false })
  @prop({ enum: GroupMembershipStatus, required: true })
  status: GroupMembershipStatus;

  // static fromPastParticipant(
  //   group: DocumentType<Group>,
  //   participant: PastGroupParticipant
  // ) {
  //   return {
  //     group,
  //     role: GroupMembershipRole.MEMBER,
  //     status: GroupMembershipStatus.LEFT,
  //     leftAt: participant.leaveTime
  //       ? new Date(participant.leaveTime)
  //       : undefined,
  //   };
  // }
  // static fromParticipant(
  //   group: DocumentType<Group>,
  //   participant: Partial<GroupParticipant>
  // ): GroupMembership {
  //   return {
  //     group,
  //     role: participant.isSuperAdmin
  //       ? GroupMembershipRole.SUPER_ADMIN
  //       : participant.isAdmin
  //         ? GroupMembershipRole.ADMIN
  //         : GroupMembershipRole.MEMBER,
  //     status: GroupMembershipStatus.JOINED,
  //   };
  // }
}
export const UserProfileModel = getModelForClass(UserProfile);
