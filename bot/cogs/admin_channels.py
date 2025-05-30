"""
Emerald's Killfeed - Admin Channel Configuration (PHASE 3)
Channel setup commands with premium gating
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands
from bot.cogs.autocomplete import ServerAutocomplete

logger = logging.getLogger(__name__)

class AdminChannels(commands.Cog):
    """
    ADMIN CHANNEL COMMANDS (PHASE 3)
    - /setchannel killfeed (FREE)
    - /setchannel leaderboard (PREMIUM)
    - /setchannel playercountvc (PREMIUM)
    - /setchannel events (PREMIUM)
    - /setchannel connections (PREMIUM)
    - /setchannel bounties (PREMIUM)
    - /clearchannels (resets all)
    """
    
    def __init__(self, bot):
        self.bot = bot
        
        # Channel types and their premium requirements
        self.channel_types = {
            'killfeed': {'premium': False, 'description': 'Real-time kill feed updates', 'type': discord.ChannelType.text},
            'leaderboard': {'premium': True, 'description': 'Automated consolidated leaderboard (30min updates)', 'type': discord.ChannelType.text},
            'playercountvc': {'premium': True, 'description': 'Live player count voice channel', 'type': discord.ChannelType.voice},
            'events': {'premium': True, 'description': 'Server events (airdrops, missions)', 'type': discord.ChannelType.text},
            'connections': {'premium': True, 'description': 'Player join/leave notifications', 'type': discord.ChannelType.text},
            'bounties': {'premium': True, 'description': 'Bounty notifications', 'type': discord.ChannelType.text}
        }
    
    async def check_premium_access(self, guild_id: int) -> bool:
        """Check if guild has any premium servers"""
        try:
            guild_doc = await self.bot.db_manager.get_guild(guild_id)
            if not guild_doc:
                return False
            
            servers = guild_doc.get('servers', [])
            for server_config in servers:
                server_id = server_config.get('server_id', server_config.get('_id', 'default'))
                if await self.bot.db_manager.is_premium_server(guild_id, server_id):
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check premium access: {e}")
            return False
    
    @discord.slash_command(name="setchannel", description="Configure output channels for the bot")
    @discord.default_permissions(administrator=True)
    async def set_channel(self, ctx,
                         channel_type: discord.Option(str, "Channel type to configure", 
                                                     choices=['killfeed', 'leaderboard', 'playercountvc', 'events', 'connections', 'bounties']),
                         channel: discord.Option(discord.abc.GuildChannel, "Channel to set (text or voice based on type)"),
                         server_id: discord.Option(str, "Server to configure channels for", required=False, default="default", autocomplete=ServerAutocomplete.autocomplete_server_name)):
        """Configure a specific channel type for a specific server"""
        try:
            guild_id = ctx.guild.id
            channel_config = self.channel_types[channel_type]
            
            # Check if channel type requires premium
            if channel_config['premium']:
                has_premium = await self.check_premium_access(guild_id)
                if not has_premium:
                    embed = discord.Embed(
                        title="🔒 Premium Feature Required",
                        description=f"Setting **{channel_type}** channel requires premium subscription!",
                        color=0xFF6B6B,
                        timestamp=datetime.now(timezone.utc)
                    )
                    
                    embed.add_field(
                        name="🎯 Free Channel",
                        value="Only **killfeed** channel is available for free users",
                        inline=False
                    )
                    
                    embed.add_field(
                        name="⭐ Premium Channels",
                        value="• **leaderboard** - Automated leaderboards\n• **playercountvc** - Live player count\n• **events** - Server events\n• **connections** - Player activity\n• **bounties** - Bounty system",
                        inline=False
                    )
                    
                    embed.add_field(
                        name="🚀 Upgrade Now",
                        value="Contact an admin to upgrade to premium!",
                        inline=False
                    )
                    
                    embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                    await ctx.respond(embed=embed, ephemeral=True)
                    return
            
            # Validate channel type
            expected_type = channel_config['type']
            if channel.type != expected_type:
                type_name = "voice" if expected_type == discord.ChannelType.voice else "text"
                embed = discord.Embed(
                    title="❌ Invalid Channel Type",
                    description=f"Channel type **{channel_type}** requires a **{type_name}** channel!",
                    color=0xFF6B6B
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            # Update guild configuration with server-specific channels
            update_field = f"server_channels.{server_id}.{channel_type}"
            await self.bot.db_manager.guilds.update_one(
                {"guild_id": guild_id},
                {
                    "$set": {
                        update_field: channel.id,
                        f"server_channels.{server_id}.{channel_type}_enabled": True,
                        f"server_channels.{server_id}.{channel_type}_updated": datetime.now(timezone.utc)
                    }
                },
                upsert=True
            )
            
            # Create success embed
            embed = discord.Embed(
                title=f"✅ {channel_type.title()} Channel Set",
                description=f"Successfully configured {channel.mention} for **{channel_type}** on server **{server_id}**!",
                color=0x00FF7F,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="📝 Description",
                value=channel_config['description'],
                inline=True
            )
            
            embed.add_field(
                name="🔄 Status",
                value="Active and monitoring",
                inline=True
            )
            
            # Add specific information based on channel type
            if channel_type == 'killfeed':
                embed.add_field(
                    name="⏱️ Update Frequency",
                    value="Real-time (every 5 minutes)",
                    inline=False
                )
            elif channel_type == 'leaderboard':
                embed.add_field(
                    name="⏱️ Update Frequency",
                    value="Automated hourly updates",
                    inline=False
                )
            elif channel_type == 'playercountvc':
                embed.add_field(
                    name="🎙️ Voice Channel",
                    value="Channel name will show live player count",
                    inline=False
                )
            
            # Set appropriate thumbnail
            thumbnails = {
                'killfeed': 'Killfeed.png',
                'leaderboard': 'Leaderboard.png',
                'events': 'Mission.png',
                'connections': 'Connections.png',
                'bounties': 'Bounty.png'
            }
            
            if channel_type in thumbnails:
                embed.set_thumbnail(url=f"attachment://{thumbnails[channel_type]}")
            
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
            logger.info(f"Set {channel_type} channel to {channel.id} in guild {guild_id}")
            
        except Exception as e:
            logger.error(f"Failed to set {channel_type} channel: {e}")
            await ctx.respond("❌ Failed to configure channel.", ephemeral=True)
    
    @discord.slash_command(name="setchannels", description="Configure multiple channels at once")
    @discord.default_permissions(administrator=True)
    async def set_channels(self, ctx,
                          killfeed_channel: discord.Option(discord.abc.GuildChannel, "Channel for killfeed updates", required=False),
                          events_channel: discord.Option(discord.abc.GuildChannel, "Channel for server events (missions, airdrops)", required=False),
                          connections_channel: discord.Option(discord.abc.GuildChannel, "Channel for player connections", required=False),
                          leaderboard_channel: discord.Option(discord.abc.GuildChannel, "Channel for leaderboard updates", required=False),
                          bounties_channel: discord.Option(discord.abc.GuildChannel, "Channel for bounty notifications", required=False),
                          server_id: discord.Option(str, "Server to configure channels for", required=False, default="default", autocomplete=ServerAutocomplete.autocomplete_server_name)):
        """Configure multiple channel types for a specific server at once"""
        try:
            guild_id = ctx.guild.id
            
            # Map provided channels to types
            channel_updates = {}
            configured_channels = []
            premium_required = []
            
            channels_to_set = [
                ('killfeed', killfeed_channel),
                ('events', events_channel), 
                ('connections', connections_channel),
                ('leaderboard', leaderboard_channel),
                ('bounties', bounties_channel)
            ]
            
            # Check premium access once
            has_premium = await self.check_premium_access(guild_id)
            
            for channel_type, channel in channels_to_set:
                if not channel:
                    continue
                    
                channel_config = self.channel_types[channel_type]
                
                # Check premium requirement
                if channel_config['premium'] and not has_premium:
                    premium_required.append(channel_type)
                    continue
                
                # Validate channel type
                expected_type = channel_config['type']
                if channel.type != expected_type:
                    type_name = "voice" if expected_type == discord.ChannelType.voice else "text"
                    await ctx.respond(f"❌ **{channel_type}** requires a **{type_name}** channel! {channel.mention} is invalid.", ephemeral=True)
                    return
                
                # Add to updates
                update_field = f"server_channels.{server_id}.{channel_type}"
                channel_updates[update_field] = channel.id
                channel_updates[f"server_channels.{server_id}.{channel_type}_enabled"] = True
                channel_updates[f"server_channels.{server_id}.{channel_type}_updated"] = datetime.now(timezone.utc)
                
                configured_channels.append(f"• **{channel_type}**: {channel.mention}")
            
            if premium_required:
                embed = discord.Embed(
                    title="🔒 Premium Features Skipped",
                    description=f"The following channels require premium subscription: **{', '.join(premium_required)}**",
                    color=0xFF6B6B,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.add_field(
                    name="🚀 Upgrade Now",
                    value="Contact an admin to upgrade to premium for advanced channels!",
                    inline=False
                )
                if configured_channels:
                    embed.add_field(
                        name="✅ Configured (Free)",
                        value="\n".join([ch for ch in configured_channels if 'killfeed' in ch]),
                        inline=False
                    )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            if not channel_updates:
                await ctx.respond("❌ No valid channels provided to configure.", ephemeral=True)
                return
            
            # Apply all updates at once
            await self.bot.db_manager.guilds.update_one(
                {"guild_id": guild_id},
                {"$set": channel_updates},
                upsert=True
            )
            
            # Create success embed
            embed = discord.Embed(
                title="✅ Channels Configured Successfully",
                description=f"Multiple channels have been configured for server **{server_id}**!",
                color=0x00FF7F,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="📋 Configured Channels",
                value="\n".join(configured_channels),
                inline=False
            )
            
            embed.add_field(
                name="🔄 Status",
                value="All channels are now active and monitoring",
                inline=False
            )
            
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
            logger.info(f"Configured {len(configured_channels)} channels for guild {guild_id}, server {server_id}")
            
        except Exception as e:
            logger.error(f"Failed to set multiple channels: {e}")
            await ctx.respond("❌ Failed to configure channels.", ephemeral=True)

    @discord.slash_command(name="clearchannels", description="Clear all configured channels")
    @discord.default_permissions(administrator=True)
    async def clear_channels(self, ctx,
                           server_id: discord.Option(str, "Server to clear channels for", required=False, default="default", autocomplete=ServerAutocomplete.autocomplete_server_name)):
        """Clear all channel configurations for a specific server"""
        try:
            guild_id = ctx.guild.id
            
            # Get current configuration
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            server_channels = guild_config.get('server_channels', {}).get(server_id, {}) if guild_config else {}
            
            if not any(server_channels.get(channel_type) for channel_type in self.channel_types.keys()):
                embed = discord.Embed(
                    title="ℹ️ No Channels Configured",
                    description=f"No channels are currently configured for server **{server_id}**.",
                    color=0x3498DB
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            # Clear all channels for this server
            clear_update = {}
            for channel_type in self.channel_types.keys():
                clear_update[f"server_channels.{server_id}.{channel_type}"] = None
                clear_update[f"server_channels.{server_id}.{channel_type}_enabled"] = False
            
            await self.bot.db_manager.guilds.update_one(
                {"guild_id": guild_id},
                {"$set": clear_update}
            )
            
            # Create confirmation embed
            embed = discord.Embed(
                title="🧹 Channels Cleared",
                description="All channel configurations have been reset to defaults.",
                color=0x00FF7F,
                timestamp=datetime.now(timezone.utc)
            )
            
            # List previously configured channels
            configured_channels = []
            for channel_type in self.channel_types.keys():
                channel_id = server_channels.get(channel_type)
                if channel_id:
                    channel = ctx.guild.get_channel(channel_id)
                    if channel:
                        configured_channels.append(f"• **{channel_type}**: {channel.mention}")
                    else:
                        configured_channels.append(f"• **{channel_type}**: #deleted-channel")
            
            if configured_channels:
                embed.add_field(
                    name="📋 Previously Configured",
                    value="\n".join(configured_channels),
                    inline=False
                )
            
            embed.add_field(
                name="🔄 Next Steps",
                value="Use `/setchannel` to reconfigure channels as needed.",
                inline=False
            )
            
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
            logger.info(f"Cleared all channel configurations for guild {guild_id}")
            
        except Exception as e:
            logger.error(f"Failed to clear channels: {e}")
            await ctx.respond("❌ Failed to clear channel configurations.", ephemeral=True)
    
    @discord.slash_command(name="channels", description="View current channel configuration")
    async def view_channels(self, ctx,
                          server_id: discord.Option(str, "Server to view channels for", required=False, default="default", autocomplete=ServerAutocomplete.autocomplete_server_name)):
        """View current channel configuration for a specific server"""
        try:
            guild_id = ctx.guild.id
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            server_channels = guild_config.get('server_channels', {}).get(server_id, {}) if guild_config else {}
            
            embed = discord.Embed(
                title="📋 Channel Configuration",
                description=f"Current channel setup for server **{server_id}**",
                color=0x3498DB,
                timestamp=datetime.now(timezone.utc)
            )
            
            # Check premium status
            has_premium = await self.check_premium_access(guild_id)
            premium_status = "✅ Active" if has_premium else "❌ Not Active"
            
            embed.add_field(
                name="⭐ Premium Status",
                value=premium_status,
                inline=True
            )
            
            # List configured channels
            configured = []
            not_configured = []
            
            for channel_type, config in self.channel_types.items():
                channel_id = server_channels.get(channel_type)
                
                if channel_id:
                    channel = ctx.guild.get_channel(channel_id)
                    if channel:
                        status = "🟢" if config['premium'] and not has_premium else "🟢"
                        configured.append(f"{status} **{channel_type}**: {channel.mention}")
                    else:
                        configured.append(f"🔴 **{channel_type}**: #deleted-channel")
                else:
                    premium_icon = "🔒" if config['premium'] else "⚪"
                    not_configured.append(f"{premium_icon} **{channel_type}**: Not set")
            
            if configured:
                embed.add_field(
                    name="✅ Configured Channels",
                    value="\n".join(configured),
                    inline=False
                )
            
            if not_configured:
                embed.add_field(
                    name="⚪ Available Channels",
                    value="\n".join(not_configured),
                    inline=False
                )
            
            embed.add_field(
                name="🔧 Management",
                value="• Use `/setchannel` to configure\n• Use `/clearchannels` to reset all",
                inline=False
            )
            
            embed.set_footer(text="🔒 = Premium Required • Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to view channels: {e}")
            await ctx.respond("❌ Failed to retrieve channel configuration.", ephemeral=True)

def setup(bot):
    bot.add_cog(AdminChannels(bot))