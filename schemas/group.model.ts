import { prop } from "@typegoose/typegoose/lib/prop";
import {
  getModelForClass,
  ModelOptions,
  index,
} from "@typegoose/typegoose/lib/typegoose";
import { Field, ObjectType } from "type-graphql";

@ObjectType()
export class TagField {
  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  tagValue?: string | null;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  tagExplanation?: string | null;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  model?: string | null;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  timestamp?: number | null;
}

@ObjectType()
@ModelOptions({ schemaOptions: { _id: false } })
export class DemographicTagField extends TagField {
  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  age?: string | null;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  gender?: string | null;
}

@ObjectType()
@ModelOptions({ schemaOptions: { _id: false } })
export class OrganizationTagField extends TagField {
  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  organizationType?: string | null;
}

@ObjectType()
@ModelOptions({ schemaOptions: { _id: false } }) // prevent automatic _id on embedded geographic docs
export class GeographicalTag {
  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  street?: string | null;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  neighborhood?: string | null;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  settlement?: string | null;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  regionalCouncil?: string | null;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  district?: string | null;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  tagExplanation?: string | null;
}

@ObjectType()
export class GroupTags {
  @Field(() => TagField, { nullable: true })
  @prop({ _id: false, type: () => TagField, required: false })
  politicalLeaning?: TagField; // e.g., "right", "left", "mixed"

  @Field(() => DemographicTagField, { nullable: true })
  @prop({ _id: false, type: () => DemographicTagField, required: false })
  demographic?: DemographicTagField; // e.g., "youth", "seniors" plus optional age & gender

  @Field(() => TagField, { nullable: true })
  @prop({ _id: false, type: () => TagField, required: false })
  topic?: TagField; // concise topical summary / purpose

  @Field(() => TagField, { nullable: true })
  @prop({ _id: false, type: () => TagField, required: false })
  region?: TagField; // region / locality if any

  @Field(() => OrganizationTagField, { nullable: true })
  @prop({ _id: false, type: () => OrganizationTagField, required: false })
  organization?: OrganizationTagField; // organization or group affiliation if any, with optional organizationType

  @Field(() => [GeographicalTag], { nullable: true })
  @prop({
    _id: false,
    type: () => [GeographicalTag],
    required: false,
    default: [],
  })
  geographic?: GeographicalTag[];

  @Field(() => TagField, { nullable: true })
  @prop({ _id: false, type: () => TagField, required: false })
  freeform?: TagField;

  @Field(() => TagField, { nullable: true })
  @prop({ _id: false, type: () => TagField, required: false })
  lifeEvent?: TagField;

  @Field(() => TagField, { nullable: true })
  @prop({ _id: false, type: () => TagField, required: false })
  strategicMarkets?: TagField;
}

@ObjectType()
@ModelOptions({ schemaOptions: { id: true } })
@index({ groupId: 1 }, { unique: true }) // Ensure unique groupId
@index({ accounts: 1 }) // For queries by account
@index({ lastActivityTimestamp: -1 }) // For recent activity queries
@index({ memberCount: -1 }) // For sorting by member count
@index({ accounts: 1, lastActivityTimestamp: -1 }) // Compound index for account activity queries
// New indexes for engagement analytics
@index({ engagementScore: -1 })
@index({ activeMemberPercentage30d: -1 })
@index({ normalizedEngagementScore: -1 })
export class Group {
  @Field(() => String, { nullable: false })
  id!: string;

  @Field(() => String, { nullable: false })
  @prop({ type: () => String })
  groupId!: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  description?: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false, index: true })
  subject?: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  imgSrc?: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  profilePictureMediaUrl?: string;

  @Field(() => GroupTags, { nullable: true })
  @prop({ _id: false, type: () => GroupTags, required: false })
  tags?: GroupTags;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  memberCount?: number;

  @Field(() => [String], { nullable: false })
  @prop({ type: () => [String], required: true })
  accounts: string[];

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  lastActivityTimestamp?: number;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  creationTimestamp?: number;

  @Field(() => Boolean, { nullable: true })
  @prop({ type: () => Boolean, required: false })
  announcementOnly?: boolean;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  memberAddMode?: string;

  @Field(() => Boolean, { nullable: true })
  @prop({ type: () => Boolean, required: false })
  membershipApproval?: boolean;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  lastTaggedTimestamp?: number;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  lastTagSignature?: string;

  @Field(() => Date, { nullable: true })
  @prop({ type: () => Date, required: false })
  joinedAt?: Date;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  inviteLink?: string;

  // Engagement metrics (rolling 30-day window)
  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  avgMessagesPerDay30d?: number;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  avgAuthorsPerDay30d?: number;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  avgReactionsPerMessage30d?: number;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  avgRepliesPerMessage30d?: number;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  activeMemberPercentage30d?: number;

  // New: number of distinct active days (with >=1 message) in last 30 days
  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  activeDays30d?: number;

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  engagementScore?: number;

  // Normalized engagement score (1-5 categorical) computed server-side
  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  normalizedEngagementScore?: number;
}

export const GroupModel = getModelForClass(Group);
