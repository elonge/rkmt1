import { prop } from "@typegoose/typegoose/lib/prop";
import { getModelForClass, index, modelOptions } from "@typegoose/typegoose";
import { Field, ObjectType, ID } from "type-graphql";
import { Types } from "mongoose";

// MessageMedia model
@ObjectType()
export class MessageMedia {
  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  fileSize!: number;

  @Field(() => String, { nullable: false })
  @prop({ type: () => String, required: true })
  mimeType!: string;

  @Field(() => String, { nullable: false })
  @prop({ type: () => String, required: true })
  mediaHash!: string;
}

// Reaction model
@ObjectType()
export class Reaction {
  @Field(() => Number, { nullable: false })
  @prop({ type: () => Number, required: true })
  timestamp!: number;

  @Field(() => String, { nullable: false })
  @prop({ type: () => String, required: true })
  senderId!: string;

  @Field(() => String, { nullable: false })
  @prop({ type: () => String, required: true })
  reaction!: string;
}

// Message model
@index({ messageId: 1 }, { unique: true })
@index({ lastMessageObjectID: 1 })
@index({ lastReactionObjectID: 1 })
@index({ lastMediaObjectID: 1 })
@index({ timestamp: 1 })
@index({ groupId: 1 })
@index({ groupId: 1, timestamp: 1 })
@ObjectType()
@modelOptions({ schemaOptions: { id: true } })
export class Message {
  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  timestamp!: number;

  @Field(() => String, { nullable: false })
  @prop({ type: () => String, required: true })
  messageId!: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  groupId: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  authorId!: string;

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  body!: string;

  @Field(() => [Reaction], { nullable: true })
  @prop({ type: () => [Reaction], default: [], _id: false })
  messageReactions!: Reaction[];

  @Field(() => MessageMedia, { nullable: true })
  @prop({ type: () => MessageMedia, required: false, _id: false })
  messageMedia?: MessageMedia;

  @prop({ type: () => Date, required: true })
  lastImported!: Date;

  @prop({ type: () => Date, required: true })
  firstImported!: Date;

  @prop({ type: () => Types.ObjectId, required: false })
  lastMessageObjectID!: Types.ObjectId;

  @prop({ type: () => Types.ObjectId, required: false })
  lastReactionObjectID!: Types.ObjectId;

  @prop({ type: () => Types.ObjectId, required: false })
  lastMediaObjectID!: Types.ObjectId;

  @Field(() => [String], { nullable: true })
  @prop({ type: () => [String], default: [], _id: false })
  messageReplies: String[]; // array of messageIds that are replies to this message

  @Field(() => String, { nullable: true })
  @prop({ type: () => String, required: false })
  quotedMessageId: string; // messageId of the message that this message replies to

  @Field(() => Number, { nullable: true })
  @prop({ type: () => Number, required: false })
  forwardingScore: number;

  // Vector embedding for semantic search on body content
  @prop({ type: () => [Number], required: false })
  body_embedding?: number[];
}

export const MessageModel = getModelForClass(Message);
