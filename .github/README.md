# GitHub Actions 工作流说明

本目录包含用于自动化构建、测试和发布 Goscan 项目的 GitHub Actions 工作流。

## 工作流概述

### 1. `build.yml` - 持续集成 (CI)

**触发条件：**
- 推送到 `main` 或 `dev` 分支
- 创建 Pull Request 到 `main` 分支

**主要功能：**
- **测试作业** (`test`):
  - 代码格式检查
  - 静态分析 (go vet)
  - 生成 Swagger 文档
  - 运行单元测试

- **构建检查** (`build-check`):
  - 验证代码能够正常编译
  - 仅构建 Linux amd64 版本做快速验证
  - 不创建发布包（节省资源）

### 2. `release.yml` - 发布流程

**触发条件：**
- 仅在推送标签时触发（`v*` 格式）

**主要功能：**
- **构建作业** (`build`):
  - 多平台构建 (Linux, Windows, macOS)
  - 支持多架构 (amd64, arm64)
  - 创建完整发布包
  - 上传构建产物

- **发布作业** (`release`):
  - 自动生成变更日志
  - 创建 GitHub Release
  - 发布所有平台的二进制文件

- **Docker 构建** (`docker`):
  - 构建多架构 Docker 镜像
  - 推送到 GitHub Container Registry

## 使用方法

### 日常开发流程

每次推送代码到 `main` 或 `dev` 分支时，会自动运行：
- 代码格式检查和静态分析
- 单元测试
- 构建验证（确保代码能正常编译）

### 创建发布版本

1. 确保代码已合并到 `main` 分支
2. 创建并推送标签：
   ```bash
   git tag -a v1.0.0 -m "Release v1.0.0"
   git push origin v1.0.0
   ```

3. GitHub Actions 会自动：
   - 构建所有平台的二进制文件
   - 生成变更日志
   - 创建 GitHub Release
   - 构建并推送 Docker 镜像

### Docker 镜像

Docker 镜像会自动推送到 GitHub Container Registry:
```bash
docker pull ghcr.io/your-username/goscan:main
docker pull ghcr.io/your-username/goscan:v1.0.0
```

## 支持的平台

- **Linux**: amd64, arm64
- **Windows**: amd64
- **macOS**: amd64, arm64

## 发布包内容

每个发布包包含：
- 编译好的二进制文件
- 配置文件模板 (`config.yaml`)
- API 文档 (`docs/`)
- 安装脚本 (`install.sh`, 仅 Unix 平台)

## 环境变量

- `GO_VERSION`: Go 语言版本（当前：1.24.3）

## 注意事项

1. **权限要求**: 确保 GitHub Actions 有权限推送标签和创建发布版本
2. **Secrets**: 使用默认的 `GITHUB_TOKEN`，无需额外配置
3. **Docker**: Docker 镜像推送需要 `packages: write` 权限
4. **CGO**: 项目启用了 CGO，确保依赖库支持交叉编译

## 故障排除

### 构建失败
- 检查 Go 版本兼容性
- 验证依赖包是否支持目标平台
- 查看具体的错误日志

### 发布失败
- 确保版本号格式正确 (v1.0.0)
- 检查标签是否已存在
- 验证 GitHub token 权限

### Docker 构建失败
- 检查 Dockerfile 语法
- 验证基础镜像可用性
- 确保多架构构建支持